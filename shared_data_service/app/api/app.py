"""Global FastAPI assembly: middleware, error handlers, health, module
routers. Receives the already-wired container — no construction here."""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import APIRouter, FastAPI

from app.api.errors import register_error_handlers
from app.api.health import build_health_router
from app.api.middleware import CorrelationIdMiddleware
from app.bootstrap.container import Container
from app.modules import ALL_SPECS
from app.modules.shared.routes import ModuleRoutes, ScopedModuleRoutes


def create_app(container: Container) -> FastAPI:
    run_consumer = container.settings.service_mode in ("consumer", "both")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await container.start()
        if run_consumer:
            container.start_consumer()
        try:
            yield
        finally:
            # stop() owns the consumer task too; nothing here can raise past
            # it and leak the engine/bus.
            await container.stop()

    app = FastAPI(
        title="Shared Data Service",
        version="0.1.0",
        description="Authoritative storage service for shared application data",
        lifespan=lifespan,
    )
    app.add_middleware(CorrelationIdMiddleware)
    register_error_handlers(app)
    app.include_router(build_health_router(container.readiness))
    # Mount order = ALL_SPECS order. A spec is either a root module (project:
    # flat /project routes) or scoped under a parent (scope_parent="project":
    # nested /{project_id}/user routes). `also_unscoped` additionally mounts
    # the flat top-level route (/user), so user is reachable both ways.
    for spec in ALL_SPECS:
        service = container.services[spec.name]
        routes_cls = spec.routes_cls or ModuleRoutes
        if spec.scope_parent is not None:
            app.include_router(ScopedModuleRoutes(spec, service).register())
            if spec.also_unscoped:
                # Explicit top-level route at the PLURAL name (e.g. /users),
                # unscoped — all rows across every parent.
                unscoped = APIRouter(prefix=f"/{spec.name}s", tags=[spec.name])
                app.include_router(routes_cls(spec, service).register(unscoped))
        else:
            app.include_router(routes_cls(spec, service).register())
    app.state.container = container
    return app
