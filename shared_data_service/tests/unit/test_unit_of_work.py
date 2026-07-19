"""SqlAlchemyUnitOfWork publish-after-commit contract, with a fake session
(the DB-backed behavior is covered by integration tests)."""
from app.database.unit_of_work import SqlAlchemyUnitOfWork
from app.messaging.cloudevents import CloudEvent
from tests.fakes import FakeEventPublisher


class FakeSession:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.expunged = False

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True

    async def close(self) -> None:
        self.closed = True

    def expunge_all(self) -> None:
        self.expunged = True


def make_uow(publisher: FakeEventPublisher) -> tuple[SqlAlchemyUnitOfWork, FakeSession]:
    session = FakeSession()
    return SqlAlchemyUnitOfWork(lambda: session, publisher), session


EVENT = CloudEvent(id="1", source="s", type="user.created")


async def test_staged_event_published_only_after_commit():
    publisher = FakeEventPublisher()
    uow, session = make_uow(publisher)
    async with uow:
        uow.stage_event(EVENT)
        assert publisher.published == []  # not before commit
        await uow.commit()
        assert publisher.published == [EVENT]
    assert session.committed and session.closed and not session.rolled_back


async def test_exception_rolls_back_and_discards_events():
    publisher = FakeEventPublisher()
    uow, session = make_uow(publisher)
    try:
        async with uow:
            uow.stage_event(EVENT)
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert publisher.published == []
    assert session.rolled_back and session.closed and not session.committed


async def test_missing_commit_rolls_back_and_detaches_reads():
    publisher = FakeEventPublisher()
    uow, session = make_uow(publisher)
    async with uow:
        uow.stage_event(EVENT)
    assert publisher.published == [] and session.rolled_back
    # Clean read-only exit detaches instances BEFORE rollback expires them,
    # so read paths can return ORM objects without a magic commit.
    assert session.expunged


async def test_exceptional_exit_does_not_detach():
    publisher = FakeEventPublisher()
    uow, session = make_uow(publisher)
    try:
        async with uow:
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert session.rolled_back and not session.expunged


async def test_commit_failure_with_staged_events_logs_critical(caplog):
    class FailingCommitSession(FakeSession):
        async def commit(self) -> None:
            raise ConnectionError("connection lost during commit")

    session = FailingCommitSession()
    uow = SqlAlchemyUnitOfWork(lambda: session, FakeEventPublisher())
    with caplog.at_level("CRITICAL"):
        try:
            async with uow:
                uow.stage_event(EVENT)
                await uow.commit()
        except ConnectionError:
            pass
    # The ambiguous-commit window must be loud: these events may be lost.
    assert any("commit failed with staged events" in r.message for r in caplog.records)


async def test_cancellation_during_publish_is_loud_and_reraises(caplog):
    # CancelledError is a BaseException: `except Exception` misses it. A
    # client disconnect mid-publish must not silently drop the remaining
    # staged events of an already-committed transaction.
    import asyncio

    class CancelledPublisher:
        async def publish_event(self, event: CloudEvent) -> None:
            raise asyncio.CancelledError()

    session = FakeSession()
    uow = SqlAlchemyUnitOfWork(lambda: session, CancelledPublisher())
    second = CloudEvent(id="2", source="s", type="user.updated")
    with caplog.at_level("CRITICAL"):
        try:
            async with uow:
                uow.stage_event(EVENT)
                uow.stage_event(second)
                await uow.commit()
            raise AssertionError("cancellation should propagate")
        except asyncio.CancelledError:
            pass
    assert session.committed  # the write stands
    record = next(r for r in caplog.records
                  if "cancelled during post-commit publish" in r.message)
    # Both unpublished events are named so an operator can recover them.
    assert record.event_ids == ["1", "2"]


async def test_mark_events_processed_empty_batch_is_a_noop():
    # .values([]) would compile to INSERT ... DEFAULT VALUES and explode at
    # execute time; an empty batch must simply report nothing new.
    uow, session = make_uow(FakeEventPublisher())
    async with uow:
        assert await uow.mark_events_processed([]) == set()


async def test_publish_failure_after_commit_is_swallowed(caplog):
    class ExplodingPublisher:
        async def publish_event(self, event: CloudEvent) -> None:
            raise ConnectionError("broker gone")

    session = FakeSession()
    uow = SqlAlchemyUnitOfWork(lambda: session, ExplodingPublisher())
    with caplog.at_level("ERROR"):
        async with uow:
            uow.stage_event(EVENT)
            await uow.commit()  # must not raise: the commit already happened
    assert session.committed
    assert any("publish failed after commit" in r.message for r in caplog.records)
