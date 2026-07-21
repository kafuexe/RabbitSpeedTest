"""Shared, overridable CRUD routes for one module.

IMPORTANT: this module must NOT have `from __future__ import annotations`.
The route parameter annotations below reference the runtime value
`spec.create` (etc.), which FastAPI evaluates eagerly at def-time to get
the real Pydantic class. Under PEP 563 those annotations would become the
string "spec.create" and every route would fail at startup with a
confusing PydanticUserError. Guarded by
test_routes_module_has_no_future_annotations.

Two layers:
- LOGIC (create / get_one / update / list): all behavior, all state via
  self.spec / self.service — THE override surface, every method
  super()-callable.
- SIGNATURE (_*_endpoint): exist only to hand FastAPI concrete per-module
  annotations. Each inner endpoint calls THROUGH self so a subclass's
  override resolves via the MRO. `# type: ignore[valid-type]` appears ONLY
  on the dynamic-annotation lines here — nowhere else in the codebase.
"""
import uuid
from typing import Annotated, Any, Callable, Generic, NamedTuple, cast

from fastapi import APIRouter, Depends, Path, Query, Request, Response, status
from pydantic import BaseModel, ValidationError

from app.modules.shared.errors import InvalidInputError, NotFoundError
from app.modules.shared.filters import LOOKUPS
from app.modules.shared.schemas import Pagination
from app.modules.shared.service import VersionedModuleService
from app.modules.shared.spec import D, ModuleSpec, M, U

_PAGINATION_PARAMS = frozenset(Pagination.model_fields)  # limit, offset, sort


class Scope(NamedTuple):
    """Confines a scoped module's CRUD to one parent: rows whose `column`
    equals `value`."""

    column: str
    value: uuid.UUID


# A FastAPI dependency that yields the request's Scope (or None when the
# route is unscoped). The scoped variant declares the `{parent}_id` path
# param; the unscoped one takes no params.
ScopeDependency = Callable[..., Any]


class ModuleRoutes(Generic[M, D, U]):
    """The four CRUD routes for one module. Instantiated from an ModuleSpec
    and its service; a module needing custom behavior subclasses this,
    overrides a logic method (calling super()) and/or `extra_routes`, and
    passes `routes_cls=` in its spec."""

    def __init__(
        self,
        spec: ModuleSpec[M, D, U],
        service: VersionedModuleService[M, D, U],
    ) -> None:
        self.spec = spec
        self.service = service

    def register(self, router: APIRouter | None = None) -> APIRouter:
        """Flat CRUD at `/{name}` (singular). Pass `unscoped_router()` to
        mount the plural, unscoped variant of a scoped module."""
        spec = self.spec
        if router is None:
            router = APIRouter(prefix=f"/{spec.name}", tags=[spec.name])
        pid = f"/{{{spec.name}_id}}"
        self._mount(router, base="", id_path=pid, suffix="",
                    scope_dep=self._no_scope_dependency())
        return router

    def register_scoped(self, router: APIRouter | None = None) -> APIRouter:
        """Nested CRUD at `/{parent}_id/{name}`, confined to one parent.
        Inherited (not a subclass), so a custom `routes_cls` composes with
        scoping — its logic overrides apply to the scoped routes too."""
        spec = self.spec
        if router is None:
            router = APIRouter(tags=[spec.name])
        base = f"/{{{self._scope_col}}}/{spec.name}"          # /{project_id}/user
        self._mount(router, base=base,
                    id_path=f"{base}/{{{spec.name}_id}}",     # …/{user_id}
                    suffix="_scoped", scope_dep=self._scope_dependency())
        return router

    def unscoped_router(self) -> APIRouter:
        """The router for a scoped module's top-level unscoped routes: the
        PLURAL name (e.g. `/users`), so the route-shape rule lives here, not
        in the composition root."""
        return APIRouter(prefix=f"/{self.spec.name}s", tags=[self.spec.name])

    @property
    def _scope_col(self) -> str:
        assert self.spec.scope_parent is not None  # only called for scoped mounts
        return f"{self.spec.scope_parent}_id"

    def _no_scope_dependency(self) -> ScopeDependency:
        async def no_scope() -> Scope | None:
            return None

        return no_scope

    def _scope_dependency(self) -> ScopeDependency:
        """A dependency that declares the `{parent}_id` path param and yields
        the Scope confining the route to that parent."""
        col = self._scope_col

        async def scope(
            scope_id: Annotated[uuid.UUID, Path(alias=col)],
        ) -> Scope | None:
            return Scope(col, scope_id)

        return scope

    def _mount(
        self, router: APIRouter, *, base: str, id_path: str, suffix: str,
        scope_dep: ScopeDependency,
    ) -> None:
        """Mount the four CRUD routes — the ONE definition of their paths,
        methods, status, response models, names, and endpoints. `register`
        and `register_scoped` differ only in `base`/`id_path`, the name
        `suffix`, and the injected `scope_dep`."""
        name, out, page = self.spec.name, self.spec.out, self.spec.page_out
        routes = (
            ("POST", base, self._create_endpoint(scope_dep), out, f"create_{name}{suffix}"),
            ("GET", id_path, self._get_endpoint(scope_dep), out, f"get_{name}{suffix}"),
            ("PATCH", id_path, self._update_endpoint(scope_dep), out, f"update_{name}{suffix}"),
            ("GET", base, self._list_endpoint(scope_dep), page, f"list_{name}{suffix}"),
        )
        is_list = 3  # index of the list route (gets the filter description)
        for i, (method, path, endpoint, response_model, route_name) in enumerate(routes):
            router.add_api_route(
                path, endpoint, methods=[method], response_model=response_model,
                name=route_name,
                status_code=status.HTTP_201_CREATED if method == "POST" else 200,
                description=self._list_description() if i == is_list else None)
        self.extra_routes(router)

    def _list_description(self) -> str:
        """Document the dynamic filter params (they are read from the raw
        query string, so FastAPI cannot auto-generate them)."""
        fields = ", ".join(sorted(self.spec.filters.model_fields)) or "(none)"
        ops = ", ".join(sorted(LOOKUPS))
        return (
            f"Filter with `field__op=value` (bare `field=value` means exact). "
            f"Filterable fields: {fields}. Operators: {ops}. "
            f"`in`/`not_in`/`range` take comma-separated values."
        )

    # -------------------------------------------------- logic (override here)
    # Every method takes an optional `scope` = (column, value): a scoped
    # mount (register_scoped) confines CRUD to rows whose scope column equals
    # the path id (create sets it, get/update 404 on mismatch, list forces
    # the filter). Flat mounts (register) pass None.

    async def create(
        self, payload: BaseModel, response: Response, *, scope: Scope | None = None
    ) -> BaseModel:
        values = payload.model_dump()
        module_id = values.pop("id", None) or uuid.uuid4()
        if scope is not None:
            values[scope.column] = scope.value
        try:
            data = self.spec.data.model_validate({**values, "id": module_id})
        except ValidationError as exc:
            # Defence, not behavior: unreachable while every Create schema
            # stays strictly stronger than its Data floor (the client 422s
            # at the Create edge first). If a future Create is looser, a
            # floor violation still surfaces as 400, never an unhandled 500.
            raise InvalidInputError(
                f"create payload violates the {self.spec.name} data floor "
                f"({exc.error_count()} error(s))"
            ) from exc
        module, created = await self.service.create(data)
        if not created:
            response.status_code = status.HTTP_200_OK
        return self.spec.out.model_validate(module)

    async def get_one(
        self, module_id: uuid.UUID, *, scope: Scope | None = None
    ) -> BaseModel:
        module = await self.service.get(module_id)
        self._check_scope(module, module_id, scope)
        return self.spec.out.model_validate(module)

    async def update(
        self, module_id: uuid.UUID, payload: BaseModel, *, scope: Scope | None = None
    ) -> BaseModel:
        # The scope check rides the service's own locked read (guard=), so a
        # scoped update is ONE fetch, not a pre-fetch plus the locked read.
        # expected_version comes from the payload itself (VersionedUpdate).
        module = await self.service.update(module_id, cast(U, payload), guard=scope)
        return self.spec.out.model_validate(module)

    def _check_scope(
        self, module: object, module_id: uuid.UUID, scope: Scope | None
    ) -> None:
        if scope is not None and getattr(module, scope.column) != scope.value:
            raise NotFoundError(f"{self.spec.name} {module_id} not found")

    async def list(
        self,
        request: Request,
        pagination: Pagination,
        *,
        scope: Scope | None = None,
    ) -> BaseModel:
        # Filters are dynamic `field__op` params read from the raw query
        # string; strip the pagination params (limit/offset/sort) and hand
        # the rest to the service, which whitelists + parses them.
        raw_filters = {
            key: value
            for key, value in request.query_params.items()
            if key.partition("__")[0] not in _PAGINATION_PARAMS
        }
        if scope is not None:
            # Force the scope filter — drop any client-supplied filter on the
            # scope column so it cannot widen past its parent.
            raw_filters = {
                k: v
                for k, v in raw_filters.items()
                if k.partition("__")[0] != scope.column
            }
            raw_filters[scope.column] = str(scope.value)
        page = await self.service.list_page(
            limit=pagination.limit, offset=pagination.offset,
            sort=pagination.sort, filters=raw_filters)
        return self.spec.page_out(
            items=[self.spec.out.model_validate(i) for i in page.items],
            total=page.total, limit=page.limit, offset=page.offset)

    def extra_routes(self, router: APIRouter) -> None:
        """Hook for endpoints beyond CRUD (no-op by default)."""

    # --------------------------------------- signature layer (annotations)
    # ONE set of four factories serves both flat and scoped routes: the
    # scope arrives as a FastAPI dependency (scope_dep), so the two share the
    # same endpoint bodies. `# type: ignore[valid-type]` is confined to the
    # dynamic-annotation lines; bodies use cast() (not a type-ignore);
    # factories return `cast(Any, endpoint)` because the closure's type is
    # partially unknown once an annotation is suppressed.

    def _create_endpoint(self, scope_dep: ScopeDependency) -> Any:
        async def endpoint(
            payload: self.spec.create,  # type: ignore[valid-type]
            response: Response,
            scope: Scope | None = Depends(scope_dep),
        ):
            return await self.create(cast(BaseModel, payload), response, scope=scope)

        return cast(Any, endpoint)

    def _get_endpoint(self, scope_dep: ScopeDependency) -> Any:
        async def endpoint(
            module_id: Annotated[uuid.UUID, Path(alias=f"{self.spec.name}_id")],
            scope: Scope | None = Depends(scope_dep),
        ):
            return await self.get_one(module_id, scope=scope)

        return cast(Any, endpoint)

    def _update_endpoint(self, scope_dep: ScopeDependency) -> Any:
        async def endpoint(
            module_id: Annotated[uuid.UUID, Path(alias=f"{self.spec.name}_id")],
            payload: self.spec.update,  # type: ignore[valid-type]
            scope: Scope | None = Depends(scope_dep),
        ):
            return await self.update(module_id, cast(BaseModel, payload), scope=scope)

        return cast(Any, endpoint)

    def _list_endpoint(self, scope_dep: ScopeDependency) -> Any:
        async def endpoint(
            request: Request,
            pagination: Annotated[Pagination, Query()],
            scope: Scope | None = Depends(scope_dep),
        ):
            return await self.list(request, pagination, scope=scope)

        return cast(Any, endpoint)
