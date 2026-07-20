"""EntitySpec — the one declaration a module makes to plug into the generic
machinery (service, repository, routing bodies, event registration, wiring).

A module is one file with explicit classes: the ORM model (columns tagged
with `q()` for query whitelists), the Data model (full state — business
model AND event payload, carrying the permissive floor), the strict API
schemas, the Out/Page/Filters response models, and one EntitySpec instance
tying them together. No dynamic class generation anywhere: everything the
spec references is a hand-written class the reader can open.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Mapping,
    Protocol,
    Self,
    TypeVar,
    cast,
)

from pydantic import BaseModel

from app.modules.shared.query import SortSpec

if TYPE_CHECKING:
    from app.messaging.batcher import Batcher
    from app.messaging.registry import EventHandlerRegistry
    from app.modules.shared.routes import EntityRoutes
    from app.modules.shared.service import VersionedEntityService


def q(*, filter: bool = False, sort: bool = False) -> dict[str, bool]:
    """Column tags for `mapped_column(info=q(...))` — the single source of
    truth for a model's query whitelists. The repository derives its
    filterable/sortable column maps from these tags (id, version,
    created_at, updated_at are always sortable), and the filter-sync test
    holds each module's Filters schema to the same tags."""
    return {"filter": filter, "sort": sort}


class VersionedEntity(Protocol):
    """What the generic choreography needs from an ORM model instance. Not
    a bound on M — pyright cannot match SQLAlchemy's Mapped[] descriptors
    against protocol members — but the service casts through this at the
    two places it touches `version`, keeping those accesses typed."""

    @property
    def id(self) -> uuid.UUID: ...

    @property
    def version(self) -> int: ...

    @version.setter
    def version(self, value: int) -> None: ...


class StateData(Protocol):
    """What the generic choreography needs from a module's `*Data` model:
    the identity/ordering fields plus the two pydantic operations the
    machinery calls. Any pydantic BaseModel declaring `id` and `version`
    satisfies this structurally."""

    @property
    def id(self) -> uuid.UUID: ...
    @property
    def version(self) -> int: ...

    @classmethod
    def model_validate(
        cls, obj: Any, *, from_attributes: bool | None = None
    ) -> Self: ...

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]: ...


M = TypeVar("M")  # ORM model (satisfies VersionedEntity at runtime)
D = TypeVar("D", bound=StateData)  # full-state payload (business + event)
U = TypeVar("U", bound=BaseModel)  # partial-update API schema


@dataclass(frozen=True)
class StateEventItem(Generic[D]):
    """One consumed event: identity for dedup + the state it announces."""

    event_id: str
    source: str
    data: D


@dataclass(frozen=True)
class EntitySpec(Generic[M, D, U]):
    """Everything the generic machinery needs to run one entity.

    `mutable_fields` drives entity construction, replay equality, and
    update application; `field_validators` is the hook for rules that
    cannot live in an Annotated type (default: empty — the pydantic models
    validate by construction); `service_cls` is the extension point for
    modules that need custom behavior (None → the generic service).
    """

    name: str
    model: type[M]
    data: type[D]
    create: type[BaseModel]
    update: type[U]
    out: type[BaseModel]
    filters: type[BaseModel]         # PURE filter model — the sync-test target
    page_out: type[BaseModel]        # named Page[out] subclass (stable schema name)
    list_params: type[BaseModel]     # <Filters, Pagination> composite for the list route
    mutable_fields: tuple[str, ...]
    default_sort: SortSpec = SortSpec(field="created_at", descending=True)
    field_validators: Mapping[str, Callable[[Any], Any]] = field(
        default_factory=lambda: {}
    )
    service_cls: type[VersionedEntityService[M, D, U]] | None = None
    # Routes come from EntityRoutes (shared/routes.py). `routes_cls` is the
    # override seam (None → the generic EntityRoutes; resolved in api_app to
    # avoid a spec↔routes import cycle, mirroring `service_cls`).
    routes_cls: type[EntityRoutes[Any, Any, Any]] | None = None
    # Nested/scoped routing. `scope_parent` = the parent entity a row belongs
    # to (e.g. "project"): routes nest under /{parent}_id and CRUD is scoped
    # to that parent via the `{parent}_id` column (which the model must have,
    # filterable). None → a top-level (root) entity like project. When
    # `also_unscoped` is set, the entity ALSO gets a top-level unscoped route
    # (e.g. /users) that ignores the scope.
    scope_parent: str | None = None
    also_unscoped: bool = False
    # Replaces the generic created/updated handler registration entirely —
    # the seam for a module whose consumption contract genuinely differs.
    # None → shared/events.register_entity_event_handlers.
    register_events: (
        Callable[
            [EntitySpec[M, D, U], EventHandlerRegistry, Batcher[StateEventItem[D]]],
            None,
        ]
        | None
    ) = None
    # Registers handlers for ADDITIONAL event types beyond created/updated.
    extra_event_handlers: (
        Callable[[EventHandlerRegistry, VersionedEntityService[M, D, U]], None]
        | None
    ) = None

    def __post_init__(self) -> None:
        # Spec-shape invariants fail at construction (import time), the one
        # depth every misdeclaration shares — and unlike asserts they
        # survive `python -O`.
        if len({self.data, self.create, self.update, self.out}) != 4:
            raise ValueError(
                f"{self.name}: data/create/update/out must be four distinct classes"
            )
        if "expected_version" in self.mutable_fields:
            raise ValueError(
                f"{self.name}: 'expected_version' is reserved for the optimistic-"
                "concurrency guard (VersionedUpdate) and cannot be a mutable field"
            )
        data_fields = cast("type[BaseModel]", self.data).model_fields
        missing = [
            name for name in self.mutable_fields if name not in data_fields
        ]
        if missing:
            raise ValueError(
                f"{self.name}: mutable_fields missing from the data model: {missing}"
            )
        # The generic repository hard-depends on these columns (id keying,
        # version guard, updated_at refresh in the upsert); missing one
        # would otherwise surface as a mid-consume SQL error classified
        # transient — an infinite redelivery loop, not a clean failure.
        columns = cast(Any, self.model).__table__.columns
        missing_columns = [
            name
            for name in ("id", "version", "created_at", "updated_at")
            if name not in columns
        ]
        if missing_columns:
            raise ValueError(
                f"{self.name}: model lacks required columns {missing_columns} "
                "(the versioned repository contract needs them)"
            )

    @property
    def created_event_type(self) -> str:
        return f"{self.name}.created"

    @property
    def updated_event_type(self) -> str:
        return f"{self.name}.updated"
