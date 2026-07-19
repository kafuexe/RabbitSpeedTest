"""UserRepository against real PostgreSQL."""
import uuid

import pytest

from functools import partial

from app.modules.shared.query import ListQuery, PageRequest, SortSpec
from app.modules.shared.repository import VersionedRepository
from app.modules.user import User
from tests.integration.conftest import requires_pg

# The repository is generic and model-configured now; bind it to User so
# the call sites below stay exactly as they were.
UserRepository = partial(VersionedRepository, User)

pytestmark = requires_pg


def user(n: int) -> User:
    return User(
        id=uuid.uuid4(), name=f"user-{n:02d}", email=f"u{n:02d}@example.com",
        attributes={"n": n}, version=1,
    )


async def test_insert_if_absent_and_get(container):
    async with container.session_factory() as session:
        repo = UserRepository(session)
        u = user(1)
        inserted = await repo.insert_if_absent(u)
        assert inserted is not None
        assert inserted.created_at is not None  # RETURNING: one round trip
        assert await repo.insert_if_absent(u) is None  # idempotent
        await session.commit()
        stored = await repo.get(u.id)
        assert stored is not None and stored.created_at is not None


async def test_get_for_update_locks_row(container):
    async with container.session_factory() as session:
        repo = UserRepository(session)
        u = user(2)
        await repo.insert_if_absent(u)
        await session.commit()
        locked = await repo.get_for_update(u.id)
        assert locked is not None and locked.id == u.id
        await session.rollback()


async def test_list_filters_sorting_pagination(container):
    async with container.session_factory() as session:
        repo = UserRepository(session)
        for n in range(10):
            await repo.insert_if_absent(user(n))
        await session.commit()

        page = PageRequest(limit=3, offset=0)
        items, total = await repo.list(
            ListQuery(page=page, sort=SortSpec("name", descending=False))
        )
        assert total == 10
        assert [u.name for u in items] == ["user-00", "user-01", "user-02"]

        items, _ = await repo.list(
            ListQuery(page=PageRequest(limit=3, offset=8), sort=SortSpec("name"))
        )
        assert [u.name for u in items] == ["user-08", "user-09"]

        items, total = await repo.list(
            ListQuery(page=page, sort=SortSpec("name"), filters={"email": "u05@example.com"})
        )
        assert total == 1 and items[0].name == "user-05"

        items, _ = await repo.list(
            ListQuery(page=page, sort=SortSpec("name", descending=True))
        )
        assert items[0].name == "user-09"


async def test_inbox_dedup_on_real_pg(container):
    from app.database.unit_of_work import SqlAlchemyUnitOfWork
    from app.messaging.publisher import NullEventPublisher

    uow_factory = lambda: SqlAlchemyUnitOfWork(container.session_factory, NullEventPublisher())
    async with uow_factory() as uow:
        assert await uow.mark_events_processed([("urn:src", "evt-1")]) == {("urn:src", "evt-1")}
        await uow.commit()
    async with uow_factory() as uow:
        fresh = await uow.mark_events_processed(
            [("urn:src", "evt-1"), ("urn:other", "evt-1")]
        )
        assert fresh == {("urn:other", "evt-1")}  # dedup is per-source
        await uow.commit()


async def test_inbox_marks_discarded_on_rollback(container):
    from app.database.unit_of_work import SqlAlchemyUnitOfWork
    from app.messaging.publisher import NullEventPublisher

    # A failed apply must NOT leave the event marked processed, or the
    # redelivery would be dropped as a duplicate (event loss). The rollback
    # happens the way it does in production: the with-block exits on error.
    uow_factory = lambda: SqlAlchemyUnitOfWork(container.session_factory, NullEventPublisher())
    with pytest.raises(RuntimeError):
        async with uow_factory() as uow:
            assert await uow.mark_events_processed([("urn:src", "evt-rb")])
            raise RuntimeError("apply failed")
    async with uow_factory() as uow:
        assert await uow.mark_events_processed([("urn:src", "evt-rb")])  # retry works
        await uow.commit()


async def test_list_offset_beyond_end_still_reports_total(container):
    # The window-count returns no rows for an empty page; the fallback COUNT
    # must still report the real total.
    async with container.session_factory() as session:
        repo = UserRepository(session)
        for n in range(5):
            await repo.insert_if_absent(user(n))
        await session.commit()
        items, total = await repo.list(
            ListQuery(page=PageRequest(limit=10, offset=100), sort=SortSpec("name"))
        )
        assert items == [] and total == 5


async def test_real_driver_data_errors_classified_permanent(container):
    # THE test the previous fix round was missing: exercise the exception the
    # asyncpg dialect ACTUALLY raises for unstorable data (it is a generic
    # DBAPIError, never sqlalchemy.exc.DataError) and assert our classifier
    # marks it permanent so the consumer acks instead of requeue-looping.
    from sqlalchemy.exc import DBAPIError

    from app.database.errors import is_permanent_data_error

    async with container.session_factory() as session:
        repo = UserRepository(session)
        bad = user(66)
        bad.name = "nul\x00byte"
        with pytest.raises(DBAPIError) as exc_info:
            await repo.insert_if_absent(bad)
        assert is_permanent_data_error(exc_info.value), (
            f"{type(exc_info.value).__name__} with orig "
            f"{type(exc_info.value.orig).__name__} was not classified permanent"
        )
        await session.rollback()

    async with container.session_factory() as session:
        repo = UserRepository(session)
        nan = user(67)
        nan.attributes = {"x": float("nan")}
        with pytest.raises(DBAPIError) as exc_info:
            await repo.insert_if_absent(nan)
        assert is_permanent_data_error(exc_info.value)
        await session.rollback()


async def test_read_without_commit_returns_usable_instances(container):
    from app.database.unit_of_work import SqlAlchemyUnitOfWork
    from app.messaging.publisher import NullEventPublisher

    async with container.session_factory() as session:
        repo = UserRepository(session)
        u = user(30)
        await repo.insert_if_absent(u)
        await session.commit()

    # Read inside a UoW, exit WITHOUT committing: the instance must remain
    # fully usable (expunged before the exit rollback, not expired by it).
    uow = SqlAlchemyUnitOfWork(container.session_factory, NullEventPublisher())
    async with uow:
        stored = await UserRepository(uow.session).get(u.id)
    assert stored is not None
    assert stored.name == u.name and stored.created_at is not None


async def test_upsert_if_newer_many_guards_versions(container):
    async with container.session_factory() as session:
        repo = UserRepository(session)
        a, b = user(20), user(21)
        await repo.upsert_if_newer_many([a, b])  # both new → inserted
        await session.commit()

    async with container.session_factory() as session:
        repo = UserRepository(session)
        newer = User(id=a.id, name="newer", email="n@x.com", attributes={}, version=3)
        stale = User(id=b.id, name="stale", email="s@x.com", attributes={}, version=1)
        await repo.upsert_if_newer_many([newer, stale])
        await session.commit()

    async with container.session_factory() as session:
        repo = UserRepository(session)
        got_a = await repo.get(a.id)
        got_b = await repo.get(b.id)
        assert (got_a.name, got_a.version) == ("newer", 3)   # applied
        assert (got_b.name, got_b.version) == ("user-21", 1)  # stale skipped
        assert got_a.updated_at > got_a.created_at  # updated_at refreshed


async def test_rollback_discards_row(container):
    async with container.session_factory() as session:
        repo = UserRepository(session)
        u = user(3)
        await repo.insert_if_absent(u)
        await session.rollback()
    async with container.session_factory() as session:
        assert await UserRepository(session).get(u.id) is None
