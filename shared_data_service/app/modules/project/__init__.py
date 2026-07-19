"""Project module.

PHASE-1 TEMP: the spec lives here (not in business.py) because
schemas.py imports ProjectDescription from business.py — defining the spec
in business.py would make that import circular. Phase 2 collapses the
package into a single app/modules/project.py (like app/modules/user.py)
and the spec moves inline.
"""
from __future__ import annotations

from typing import cast

from fastapi import APIRouter

from app.messaging.batcher import Batcher
from app.messaging.registry import EventHandlerRegistry
from app.modules.project.business import (
    ProjectChanges,
    ProjectData,
    ProjectEventItem,
    ProjectService,
)
from app.modules.project.events import register_project_event_handlers
from app.modules.project.model import Project
from app.modules.project.router import build_project_router
from app.modules.project.schemas import (
    ProjectCreate,
    ProjectFilters,
    ProjectOut,
)
from app.modules.shared.service import VersionedEntityService
from app.modules.shared.spec import EntitySpec


def _project_router_factory(
    service: VersionedEntityService[Project, ProjectData, ProjectChanges],
) -> APIRouter:
    # The spec's router_factory is typed over the base service
    # (contravariance); the wiring built a ProjectService, so the cast is
    # exact. See modules/shared/spec.py.
    return build_project_router(cast(ProjectService, service))


def _project_register_events(
    spec: EntitySpec[Project, ProjectData, ProjectChanges],
    registry: EventHandlerRegistry,
    batcher: Batcher[ProjectEventItem],
) -> None:
    # PHASE-1 TEMP: route created/updated through the legacy permissive
    # handler (ProjectEventData floor + model_construct); phase 2 drops
    # this and the generic registration takes over.
    register_project_event_handlers(registry, batcher)


PROJECT_SPEC = EntitySpec(
    name="project",
    model=Project,
    data=ProjectData,
    create=ProjectCreate,
    update=ProjectChanges,
    out=ProjectOut,
    filters=ProjectFilters,
    mutable_fields=("name", "description", "owner_email", "attributes"),
    router_factory=_project_router_factory,
    service_cls=ProjectService,
    register_events=_project_register_events,
)
