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
)

from pydantic import BaseModel

from app.modules.shared.query import SortSpec

if TYPE_CHECKING:
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

    @property
    def model_fields_set(self) -> set[str]: ...


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
    filters: type[BaseModel]
    mutable_fields: tuple[str, ...]
    default_sort: SortSpec = SortSpec(field="created_at", descending=True)
    field_validators: Mapping[str, Callable[[Any], Any]] = field(
        default_factory=lambda: {}
    )
    service_cls: "type[VersionedEntityService[M, D, U]] | None" = None

    @property
    def created_event_type(self) -> str:
        return f"{self.name}.created"

    @property
    def updated_event_type(self) -> str:
        return f"{self.name}.updated"
