"""Composition root. The ONLY place where concrete classes meet each other;
everything else depends on constructor-injected protocols.

Two object graphs share one codebase:
- API graph      → UnitOfWork carries QueueEventPublisher (publish after commit)
- consumer graph → UnitOfWork carries NullEventPublisher  (never republishes)
"""
from __future__ import annotations

import asyncio
import logging
from functools import partial

from sqlalchemy import text

from app.config.settings import Settings
from app.database.engine import create_engine, create_session_factory
from app.database.unit_of_work import SqlAlchemyUnitOfWork
from app.logging.setup import setup_logging
from app.messaging.batcher import Batcher
from app.messaging.consumer import EventConsumer
from app.messaging.publisher import NullEventPublisher, QueueEventPublisher
from app.messaging.registry import EventHandlerRegistry
from app.messaging.rabbit_client_adapter import RabbitClientAdapter
from app.modules.project import PROJECT_SPEC
from app.modules.project.events import register_project_event_handlers
from app.modules.shared.wiring import build_entity_consumer, build_entity_service
from app.modules.user import USER_SPEC

logger = logging.getLogger(__name__)


class Container:
    def __init__(self, settings: Settings) -> None:
        setup_logging(settings.log_level)
        self.settings = settings

        # Infrastructure
        self.engine = create_engine(settings)
        self.session_factory = create_session_factory(self.engine)
        self.bus = RabbitClientAdapter(
            settings.effective_amqp_url,
            prefetch=settings.prefetch,
            persistent=settings.persistent_messages,
        )

        # API graph — committed events are published to the outbound queue.
        api_uow_factory = partial(
            SqlAlchemyUnitOfWork,
            self.session_factory,
            QueueEventPublisher(self.bus, settings.publish_queue),
        )
        self.user_service = build_entity_service(
            USER_SPEC, api_uow_factory,
            event_source=settings.event_source,
            max_page_size=settings.max_page_size,
        )
        self.project_service = build_entity_service(
            PROJECT_SPEC, api_uow_factory,
            event_source=settings.event_source,
            max_page_size=settings.max_page_size,
        )

        # Consumer graph — identical business code, publishing suppressed.
        self._build_consumer_graph()
        self._consumer_task: asyncio.Task[None] | None = None

    def _build_consumer_graph(self) -> None:
        consumer_uow_factory = partial(
            SqlAlchemyUnitOfWork, self.session_factory, NullEventPublisher()
        )
        self.registry = EventHandlerRegistry()
        self.user_batcher = build_entity_consumer(
            USER_SPEC, consumer_uow_factory, self.registry,
            event_source=self.settings.event_source,
            max_page_size=self.settings.max_page_size,
            max_batch=self.settings.consumer_batch_size,
        )
        # PHASE-1 TEMP: project still registers through its permissive
        # ProjectEventData handler (see modules/project/events.py); phase 2
        # collapses this to one build_entity_consumer line like user's.
        consumer_project_service = build_entity_service(
            PROJECT_SPEC, consumer_uow_factory,
            event_source=self.settings.event_source,
            max_page_size=self.settings.max_page_size,
        )
        self.project_batcher = Batcher(
            consumer_project_service.apply_state_events,
            max_batch=self.settings.consumer_batch_size,
        )
        register_project_event_handlers(self.registry, self.project_batcher)
        # Every module's batcher goes in this list; start()'s restart check
        # and stop()'s close loop cover them all without per-module edits.
        self._batchers = [self.user_batcher, self.project_batcher]
        self.event_consumer = EventConsumer(
            self.bus, self.registry, self.settings.consume_queues
        )

    async def start(self) -> None:
        if any(batcher.closed for batcher in self._batchers):
            # Restarting after stop(): a closed batcher fails every submit
            # with BatcherClosedError — the consumer would look healthy while
            # nacking everything forever. Rebuild the consumer graph instead.
            self._build_consumer_graph()
        await self.bus.connect()
        logger.info(
            "container started",
            extra={"mode": self.settings.service_mode,
                   "queues": self.settings.consume_queues},
        )

    def start_consumer(self) -> asyncio.Task[None]:
        """Start (and own) the supervised consumer task. Its death is loud
        and visible in readiness — never a silent stop."""
        self._consumer_task = asyncio.create_task(
            self.event_consumer.run(), name="event-consumer"
        )
        self._consumer_task.add_done_callback(self._on_consumer_done)
        return self._consumer_task

    @staticmethod
    def _on_consumer_done(task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        # A clean return is just as dead as a crash: run() parks forever on a
        # real bus, so ANY uncancelled completion means no events are being
        # consumed and deserves the same page.
        logger.critical(
            "event consumer stopped — no events are being consumed",
            exc_info=task.exception(),
        )

    async def stop(self) -> None:
        # Order matters: stop pulling new deliveries, then fail pending
        # batch items (they nack+requeue while the channel is still open),
        # then close the bus, then the pool.
        if self._consumer_task is not None and not self._consumer_task.done():
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass  # already logged by _on_consumer_done
        for batcher in self._batchers:
            await batcher.close()
        await self.bus.close()
        await self.engine.dispose()
        logger.info("container stopped")

    async def readiness(self) -> dict[str, bool]:
        db_ok = False
        try:
            async with self.engine.connect() as conn:
                await asyncio.wait_for(conn.execute(text("SELECT 1")), timeout=2.0)
            db_ok = True
        except Exception:
            logger.warning("readiness: database check failed", exc_info=True)
        checks = {"database": db_ok, "rabbitmq": self.bus.is_connected()}
        if self.settings.service_mode in ("consumer", "both"):
            checks["consumer"] = (
                self._consumer_task is not None and not self._consumer_task.done()
            )
        return checks
