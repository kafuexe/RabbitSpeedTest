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
from typing import Annotated, Any, Generic, cast

from fastapi import APIRouter, Path, Query, Request, Response, status
from pydantic import BaseModel, ValidationError

from app.modules.shared.errors import InvalidInputError, NotFoundError
from app.modules.shared.filters import LOOKUPS
from app.modules.shared.schemas import Pagination, VersionedUpdate
from app.modules.shared.service import VersionedModuleService
from app.modules.shared.spec import D, ModuleSpec, M, U

_PAGINATION_PARAMS = frozenset(Pagination.model_fields)  # limit, offset, sort

# (scope column, value) — confines a scoped module's CRUD to one parent.
Scope = tuple[str, uuid.UUID]


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
        spec = self.spec
        if router is None:
            # CHANGE 1: singular paths/tags, derived straight from spec.name.
            router = APIRouter(prefix=f"/{spec.name}", tags=[spec.name])
        pid = f"/{{{spec.name}_id}}"
        router.add_api_route(
            "", self._create_endpoint(), methods=["POST"],
            response_model=spec.out, status_code=status.HTTP_201_CREATED,
            name=f"create_{spec.name}")
        router.add_api_route(
            pid, self._get_endpoint(), methods=["GET"],
            response_model=spec.out, name=f"get_{spec.name}")
        router.add_api_route(
            pid, self._update_endpoint(), methods=["PATCH"],
            response_model=spec.out, name=f"update_{spec.name}")
        router.add_api_route(
            "", self._list_endpoint(), methods=["GET"],
            response_model=spec.page_out, name=f"list_{spec.name}",
            description=self._list_description())
        self.extra_routes(router)
        return router

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
    # Every method takes an optional `scope` = (column, value): when a
    # ScopedModuleRoutes serves /{parent}_id/<name>, it confines CRUD to
    # rows whose scope column equals the path id (create sets it, get/update
    # 404 on mismatch, list forces the filter). Unscoped routes pass None.

    async def create(
        self, payload: BaseModel, response: Response, *, scope: Scope | None = None
    ) -> BaseModel:
        values = payload.model_dump()
        module_id = values.pop("id", None) or uuid.uuid4()
        if scope is not None:
            values[scope[0]] = scope[1]
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
        if scope is not None:
            # Confirm the row is in scope before mutating it (a cross-scope
            # id is a 404, not a silent update of someone else's row).
            self._check_scope(await self.service.get(module_id), module_id, scope)
        # Every Update schema inherits VersionedUpdate → expected_version.
        ev = cast(VersionedUpdate, payload).expected_version
        module = await self.service.update(
            module_id, cast(U, payload), expected_version=ev)
        return self.spec.out.model_validate(module)

    def _check_scope(
        self, module: object, module_id: uuid.UUID, scope: Scope | None
    ) -> None:
        if scope is not None and getattr(module, scope[0]) != scope[1]:
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
                if k.partition("__")[0] != scope[0]
            }
            raw_filters[scope[0]] = str(scope[1])
        page = await self.service.list_page(
            limit=pagination.limit, offset=pagination.offset,
            sort=pagination.sort, filters=raw_filters)
        return self.spec.page_out(
            items=[self.spec.out.model_validate(i) for i in page.items],
            total=page.total, limit=page.limit, offset=page.offset)

    def extra_routes(self, router: APIRouter) -> None:
        """Hook for endpoints beyond CRUD (no-op by default)."""

    # --------------------------------------- signature layer (annotations)
    # Each factory defines an inner endpoint with a concrete per-module
    # annotation and returns it. Uniform shape: `# type: ignore[valid-type]`
    # is confined to the dynamic-annotation lines; bodies use cast() (not a
    # type-ignore); every factory returns `cast(Any, endpoint)` because the
    # closure's type is partially unknown once an annotation is suppressed.
    # `self.spec` is read inline (in the annotation itself) — self is
    # captured by the closure, so no local alias is needed.

    def _create_endpoint(self) -> Any:
        async def endpoint(payload: self.spec.create, response: Response):  # type: ignore[valid-type]
            return await self.create(cast(BaseModel, payload), response)

        return cast(Any, endpoint)

    def _get_endpoint(self) -> Any:
        async def endpoint(
            module_id: Annotated[uuid.UUID, Path(alias=f"{self.spec.name}_id")],
        ):
            return await self.get_one(module_id)

        return cast(Any, endpoint)

    def _update_endpoint(self) -> Any:
        async def endpoint(
            module_id: Annotated[uuid.UUID, Path(alias=f"{self.spec.name}_id")],
            payload: self.spec.update,  # type: ignore[valid-type]
        ):
            return await self.update(module_id, cast(BaseModel, payload))

        return cast(Any, endpoint)

    def _list_endpoint(self) -> Any:
        # No dynamic annotation here: pagination is the concrete shared
        # Pagination model, and the filters come from the raw Request.
        async def endpoint(
            request: Request, pagination: Annotated[Pagination, Query()]
        ):
            return await self.list(request, pagination)

        return endpoint


class ScopedModuleRoutes(ModuleRoutes[M, D, U]):
    """CRUD nested under a parent scope: `/{parent}_id/<name>` (e.g.
    `/{project_id}/user`). Every route is confined to rows whose scope
    column (`{parent}_id`) equals the path id — create sets it, get/update
    404 on a cross-scope id, list forces the filter. `spec.scope_parent`
    names the parent; the scope column and path param are `{parent}_id`.

    Route names are suffixed `_scoped` so they never collide with an
    module's top-level unscoped routes (see `also_unscoped`)."""

    @property
    def _scope_col(self) -> str:
        assert self.spec.scope_parent is not None  # api_app only wires scoped specs here
        return f"{self.spec.scope_parent}_id"

    def register(self, router: APIRouter | None = None) -> APIRouter:
        spec = self.spec
        if router is None:
            router = APIRouter(tags=[spec.name])
        base = f"/{{{self._scope_col}}}/{spec.name}"          # /{project_id}/user
        eid = f"{base}/{{{spec.name}_id}}"                    # …/{user_id}
        router.add_api_route(
            base, self._scoped_create_endpoint(), methods=["POST"],
            response_model=spec.out, status_code=status.HTTP_201_CREATED,
            name=f"create_{spec.name}_scoped")
        router.add_api_route(
            eid, self._scoped_get_endpoint(), methods=["GET"],
            response_model=spec.out, name=f"get_{spec.name}_scoped")
        router.add_api_route(
            eid, self._scoped_update_endpoint(), methods=["PATCH"],
            response_model=spec.out, name=f"update_{spec.name}_scoped")
        router.add_api_route(
            base, self._scoped_list_endpoint(), methods=["GET"],
            response_model=spec.page_out, name=f"list_{spec.name}_scoped",
            description=self._list_description())
        self.extra_routes(router)
        return router

    def _scoped_create_endpoint(self) -> Any:
        async def endpoint(
            scope_id: Annotated[uuid.UUID, Path(alias=self._scope_col)],
            payload: self.spec.create,  # type: ignore[valid-type]
            response: Response,
        ):
            return await self.create(
                cast(BaseModel, payload), response,
                scope=(self._scope_col, scope_id))

        return cast(Any, endpoint)

    def _scoped_get_endpoint(self) -> Any:
        async def endpoint(
            scope_id: Annotated[uuid.UUID, Path(alias=self._scope_col)],
            module_id: Annotated[uuid.UUID, Path(alias=f"{self.spec.name}_id")],
        ):
            return await self.get_one(module_id, scope=(self._scope_col, scope_id))

        return cast(Any, endpoint)

    def _scoped_update_endpoint(self) -> Any:
        async def endpoint(
            scope_id: Annotated[uuid.UUID, Path(alias=self._scope_col)],
            module_id: Annotated[uuid.UUID, Path(alias=f"{self.spec.name}_id")],
            payload: self.spec.update,  # type: ignore[valid-type]
        ):
            return await self.update(
                module_id, cast(BaseModel, payload),
                scope=(self._scope_col, scope_id))

        return cast(Any, endpoint)

    def _scoped_list_endpoint(self) -> Any:
        async def endpoint(
            scope_id: Annotated[uuid.UUID, Path(alias=self._scope_col)],
            request: Request,
            pagination: Annotated[Pagination, Query()],
        ):
            return await self.list(
                request, pagination, scope=(self._scope_col, scope_id))

        return cast(Any, endpoint)
