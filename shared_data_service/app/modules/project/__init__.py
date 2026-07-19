"""Project module.

PHASE-1 TEMP: the spec lives here (not in business.py) because
schemas.py imports ProjectDescription from business.py — defining the spec
in business.py would make that import circular. Phase 2 collapses the
package into a single app/modules/project.py (like app/modules/user.py)
and the spec moves inline.
"""
from __future__ import annotations

from app.modules.project.business import (
    ProjectChanges,
    ProjectData,
    ProjectService,
)
from app.modules.project.model import Project
from app.modules.project.schemas import (
    ProjectCreate,
    ProjectFilters,
    ProjectOut,
)
from app.modules.shared.spec import EntitySpec

PROJECT_SPEC = EntitySpec(
    name="project",
    model=Project,
    data=ProjectData,
    create=ProjectCreate,
    update=ProjectChanges,
    out=ProjectOut,
    filters=ProjectFilters,
    mutable_fields=("name", "description", "owner_email", "attributes"),
    service_cls=ProjectService,
)
