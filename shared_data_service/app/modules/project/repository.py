"""Project DAL — declares the table and its query whitelists; every mechanism
(idempotent insert, row-locked read, version-guarded bulk upsert, whitelisted
list) is inherited from the shared VersionedRepository."""
from __future__ import annotations

from app.modules.shared.repository import VersionedRepository
from app.modules.project.model import Project


class ProjectRepository(VersionedRepository[Project]):
    model = Project
    filterable_columns = {
        "name": Project.name,
        "owner_email": Project.owner_email,
    }
    sortable_columns = {
        "id": Project.id,
        "name": Project.name,
        "owner_email": Project.owner_email,
        "version": Project.version,
        "created_at": Project.created_at,
        "updated_at": Project.updated_at,
    }
