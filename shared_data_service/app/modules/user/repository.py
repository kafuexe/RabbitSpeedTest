"""User DAL — declares the table and its query whitelists; every mechanism
(idempotent insert, row-locked read, version-guarded bulk upsert, whitelisted
list) is inherited from the shared VersionedRepository."""
from __future__ import annotations

from app.modules.shared.repository import VersionedRepository
from app.modules.user.model import User


class UserRepository(VersionedRepository[User]):
    model = User
    # Single source of truth for query whitelists; the business layer derives
    # its allowed-field sets from these keys.
    filterable_columns = {
        "name": User.name,
        "email": User.email,
    }
    sortable_columns = {
        "id": User.id,
        "name": User.name,
        "email": User.email,
        "version": User.version,
        "created_at": User.created_at,
        "updated_at": User.updated_at,
    }
