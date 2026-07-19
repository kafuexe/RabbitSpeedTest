"""Composition helpers: one line per entity in the container.

Two helpers, not one, because the container builds two object graphs from
the same spec — the API graph in __init__ and the consumer graph in
_build_consumer_graph, which is REBUILT on restart-after-stop — so each
side must be wireable independently.
"""
from __future__ import annotations

from typing import cast

from app.database.unit_of_work import UnitOfWorkFactory
from app.messaging.batcher import Batcher
from app.messaging.registry import EventHandlerRegistry
from app.modules.shared.events import register_entity_event_handlers
from app.modules.shared.service import VersionedEntityService
from app.modules.shared.spec import D, EntitySpec, M, StateEventItem, U


def build_entity_service(
    spec: EntitySpec[M, D, U],
    uow_factory: UnitOfWorkFactory,
    *,
    event_source: str,
    max_page_size: int,
) -> VersionedEntityService[M, D, U]:
    """Instantiate the spec's service (custom `service_cls` or the generic
    default) over the given unit-of-work factory."""
    service_cls = spec.service_cls
    if service_cls is None:
        # The bare generic class object erases its parameters; the cast just
        # restates what instantiating it with this spec means.
        service_cls = cast(
            "type[VersionedEntityService[M, D, U]]", VersionedEntityService
        )
    return service_cls(
        spec, uow_factory, event_source=event_source, max_page_size=max_page_size
    )


def build_entity_consumer(
    spec: EntitySpec[M, D, U],
    uow_factory: UnitOfWorkFactory,
    registry: EventHandlerRegistry,
    *,
    event_source: str,
    max_page_size: int,
    max_batch: int,
) -> Batcher[StateEventItem[D]]:
    """Wire one entity's consumer side: service on the (null-publishing)
    unit of work, batcher, and handler registration. Both event seams come
    from the spec, so the container's loop has no per-entity cases."""
    service = build_entity_service(
        spec, uow_factory, event_source=event_source, max_page_size=max_page_size
    )
    batcher: Batcher[StateEventItem[D]] = Batcher(
        service.apply_state_events, max_batch=max_batch
    )
    if spec.register_events is not None:
        spec.register_events(spec, registry, batcher)
    else:
        register_entity_event_handlers(spec, registry, batcher)
    if spec.extra_event_handlers is not None:
        spec.extra_event_handlers(registry, service)
    return batcher
