"""Guard tests for the shared ModuleRoutes (app/modules/shared/routes.py).

These protect the two fragile properties the dynamic-annotation design
rests on: the OpenAPI it generates must match a hand-written concrete
router byte-for-byte (the regression guard for the `# type: ignore` lines),
and a subclass override must dispatch through super() via the MRO. Both run
against in-memory fakes — no DB.
"""
from __future__ import annotations

import uuid
from pathlib import Path as FsPath
from typing import Annotated

from fastapi import APIRouter, FastAPI, Query, Response, status
from fastapi.testclient import TestClient

from app.modules.shared.routes import ModuleRoutes
from app.modules.user import (
    USER_SPEC,
    UserCreate,
    UserListParams,
    UserOut,
    UserPageOut,
    UserService,
    UserUpdate,
)
from tests.fakes import FakeWorld


def make_user_service() -> UserService:
    world = FakeWorld()
    return UserService(
        USER_SPEC, world.uow_factory, repo_factory=world.repo_factory,
        event_source="urn:test", max_page_size=100,
    )


# ---------------------------------------------------- no-future-import guard


def test_routes_module_has_no_future_annotations() -> None:
    # routes.py MUST NOT use `from __future__ import annotations`: PEP 563
    # would turn `payload: spec.create` into the string "spec.create" and
    # every route would fail at startup with a PydanticUserError. Match the
    # exact IMPORT LINE, not the docstring that names it.
    routes_path = (
        FsPath(__file__).resolve().parents[2]
        / "app" / "modules" / "shared" / "routes.py"
    )
    lines = routes_path.read_text().splitlines()
    assert not any(
        line.strip() == "from __future__ import annotations" for line in lines
    )


# ------------------------------------------------ OpenAPI equivalence guard


def _handwritten_user_router() -> APIRouter:
    """A concrete, explicitly-typed user router at the singular paths — the
    reference ModuleRoutes must reproduce."""
    router = APIRouter(prefix="/user", tags=["user"])

    @router.post("", response_model=UserOut, status_code=status.HTTP_201_CREATED,
                 name="create_user")
    async def create_user(payload: UserCreate, response: Response) -> UserOut: ...

    @router.get("/{user_id}", response_model=UserOut, name="get_user")
    async def get_user(user_id: uuid.UUID) -> UserOut: ...

    @router.patch("/{user_id}", response_model=UserOut, name="update_user")
    async def update_user(user_id: uuid.UUID, payload: UserUpdate) -> UserOut: ...

    @router.get("", response_model=UserPageOut, name="list_user")
    async def list_user(params: Annotated[UserListParams, Query()]) -> UserPageOut: ...

    return router


def _openapi(router: APIRouter) -> dict:
    app = FastAPI()
    app.include_router(router)
    return app.openapi()


def _route_signature(spec: dict) -> dict:
    """The byte-compat-relevant slice of one path+method's OpenAPI."""
    paths = spec["paths"]
    out = {}
    for path, methods in paths.items():
        for method, op in methods.items():
            body_ref = (
                op.get("requestBody", {})
                .get("content", {})
                .get("application/json", {})
                .get("schema", {})
                .get("$ref")
            )
            responses = {
                code: r.get("content", {}).get("application/json", {})
                .get("schema", {})
                for code, r in op.get("responses", {}).items()
            }
            out[(path, method)] = {
                "operationId": op["operationId"],
                "requestBody": body_ref,
                "responses": responses,
            }
    return out


def test_module_routes_openapi_matches_handwritten() -> None:
    generated = _openapi(ModuleRoutes(USER_SPEC, make_user_service()).register())
    handwritten = _openapi(_handwritten_user_router())
    # paths, operationIds, request-body $refs, response schemas, status codes
    assert _route_signature(generated) == _route_signature(handwritten)
    # request/response component schemas are identical objects too
    for name in ("UserCreate", "UserUpdate", "UserOut", "UserPageOut"):
        assert (
            generated["components"]["schemas"][name]
            == handwritten["components"]["schemas"][name]
        ), name


# --------------------------------------------- subclass super() dispatch guard


def test_module_routes_subclass_override_calls_super() -> None:
    recorded: list[str] = []

    class RecordingUserRoutes(ModuleRoutes[object, object, object]):
        async def create(self, payload, response):  # type: ignore[override]
            result = await super().create(payload, response)
            recorded.append(str(result.id))  # side effect proving super() ran
            return result

    body = {
        "id": str(uuid.uuid4()), "name": "Ada Lovelace",
        "email": "ada@example.com", "attributes": {"role": "eng"},
    }

    base = FastAPI()
    base.include_router(ModuleRoutes(USER_SPEC, make_user_service()).register())
    sub = FastAPI()
    sub.include_router(RecordingUserRoutes(USER_SPEC, make_user_service()).register())

    base_resp = TestClient(base).post("/user", json=body)
    sub_resp = TestClient(sub).post("/user", json=body)

    assert recorded == [body["id"]]  # (a) the override's super() call ran
    assert base_resp.status_code == sub_resp.status_code == 201
    # (b) response byte-identical apart from the two server-stamped times
    #     (independent in-memory stores stamp created_at/updated_at differently)
    drop = {"created_at", "updated_at"}
    assert (
        {k: v for k, v in base_resp.json().items() if k not in drop}
        == {k: v for k, v in sub_resp.json().items() if k not in drop}
    )
