"""Mock tests for the user business service: idempotency, versioning,
ordering, and publish-after-commit — all against in-memory fakes."""
import uuid

import pytest
from pydantic import ValidationError

from app.modules.shared.errors import ConflictError, InvalidInputError, NotFoundError
from app.modules.shared.query import InvalidQueryError
from app.modules.shared.spec import StateEventItem
from app.modules.user import USER_SPEC, UserData, UserService, UserUpdate
from tests.fakes import FakeWorld

UserEventItem = StateEventItem[UserData]

# The event-type names now derive from the spec's module name.
USER_CREATED = USER_SPEC.created_event_type
USER_UPDATED = USER_SPEC.updated_event_type
# UserUpdate (the strict API schema) IS the service's changes type now;
# the old UserChanges dataclass-style model is gone.
UserChanges = UserUpdate

UID = uuid.UUID("00000000-0000-0000-0000-000000000001")


async def apply_one(service: UserService, event_id: str, source: str, data: UserData):
    """Single-event convenience for tests; production always batches."""
    await service.apply_state_events([UserEventItem(event_id, source, data)])


def make_service(world: FakeWorld) -> UserService:
    return UserService(
        USER_SPEC,
        world.uow_factory,
        repo_factory=world.repo_factory,
        event_source="urn:test",
        max_page_size=100,
    )


def data(name: str = "Alice", email: str = "alice@example.com", version: int = 1) -> UserData:
    return UserData(id=UID, name=name, email=email, attributes={"team": "a"}, version=version)


async def test_create_publishes_event_after_commit():
    world = FakeWorld()
    user, created = await make_service(world).create(data())
    assert created and user.version == 1
    assert world.uows[0].committed
    assert [e.type for e in world.publisher.published] == [USER_CREATED]
    assert world.publisher.published[0].data["id"] == str(UID)


async def test_create_replay_is_idempotent_and_reannounces():
    world = FakeWorld()
    service = make_service(world)
    await service.create(data())
    user, created = await service.create(data())
    assert not created and user.id == UID
    assert len(world.store) == 1  # no duplicate row
    # The replay re-announces the state so an event lost to an ambiguous
    # commit is recovered on retry; consumers drop it via the version guard.
    assert [e.type for e in world.publisher.published] == [USER_CREATED, USER_CREATED]
    assert world.publisher.published[-1].data["version"] == 1


async def test_create_same_id_different_content_conflicts():
    world = FakeWorld()
    service = make_service(world)
    await service.create(data())
    with pytest.raises(ConflictError):
        await service.create(data(name="Mallory"))


async def test_create_same_id_different_attributes_conflicts():
    world = FakeWorld()
    service = make_service(world)
    await service.create(data())
    with pytest.raises(ConflictError):
        await service.create(
            UserData(id=UID, name="Alice", email="alice@example.com",
                     attributes={"team": "DIFFERENT"})
        )


def test_business_data_validates_at_construction():
    # The business floor IS the model: building an invalid UserData raises
    # pydantic.ValidationError before any service call can happen (it used
    # to be an InvalidInputError raised inside service.create()).
    with pytest.raises(ValidationError):
        data(name="   ")
    with pytest.raises(ValidationError):
        data(email="not-an-email")
    with pytest.raises(ValidationError):
        UserChanges(email="ops@backend")  # business email stays STRICT


def test_business_data_validates_on_assignment():
    # validate_assignment: mutating a business model re-runs the same shared
    # rules automatically — no manual validation call anywhere.
    user = data()
    with pytest.raises(ValidationError):
        user.name = "   "
    with pytest.raises(ValidationError):
        user.email = "not-an-email"
    with pytest.raises(ValidationError):
        user.attributes = {"k": "\x00"}
    user.name = "Alice B"  # valid assignment still works
    assert user.name == "Alice B"


async def test_update_bumps_version_and_publishes():
    world = FakeWorld()
    service = make_service(world)
    await service.create(data())
    user = await service.update(UID, UserChanges(name="Alice B"))
    assert user.version == 2 and user.name == "Alice B"
    assert [e.type for e in world.publisher.published] == [USER_CREATED, USER_UPDATED]
    assert world.publisher.published[-1].data["version"] == 2


async def test_update_expected_version_conflict():
    world = FakeWorld()
    service = make_service(world)
    await service.create(data())
    with pytest.raises(ConflictError):
        await service.update(UID, UserChanges(name="X"), expected_version=99)


async def test_update_requires_changes_and_existing_user():
    world = FakeWorld()
    service = make_service(world)
    with pytest.raises(InvalidInputError):
        await service.update(UID, UserChanges())
    with pytest.raises(NotFoundError):
        await service.update(UID, UserChanges(name="X"))


async def test_get_missing_raises_not_found():
    with pytest.raises(NotFoundError):
        await make_service(FakeWorld()).get(UID)


async def test_list_rejects_bad_sort_and_filter_params():
    service = make_service(FakeWorld())
    with pytest.raises(InvalidQueryError):
        await service.list_page(limit=10, offset=0, sort="password")
    with pytest.raises(InvalidQueryError):
        await service.list_page(limit=0, offset=0)
    with pytest.raises(InvalidQueryError):
        await service.list_page(limit=101, offset=0)  # over max_page_size


# ---------------------------------------------------------- consumer path


async def test_apply_event_creates_user_and_publishes_nothing():
    world = FakeWorld()
    await apply_one(make_service(world), "evt-1", "urn:other", data())
    assert world.store[UID].name == "Alice"
    assert world.publisher.published == []  # consumer path never republishes


async def test_apply_event_duplicate_delivery_skipped():
    world = FakeWorld()
    service = make_service(world)
    await apply_one(service, "evt-1", "urn:other", data())
    await apply_one(service, "evt-1", "urn:other", data(name="Changed"))
    assert world.store[UID].name == "Alice"  # duplicate had no effect


async def test_apply_event_stale_version_skipped():
    world = FakeWorld()
    service = make_service(world)
    await apply_one(service, "evt-1", "urn:other", data(version=3))
    await apply_one(service, "evt-2", "urn:other", data(name="Old", version=2))
    assert world.store[UID].name == "Alice" and world.store[UID].version == 3


async def test_apply_event_out_of_order_update_before_create():
    world = FakeWorld()
    service = make_service(world)
    # update (v2) arrives first → upserted from full event state
    await apply_one(service, "evt-2", "urn:other", data(name="New", version=2))
    assert world.store[UID].version == 2
    # the late create (v1) is stale → dropped
    await apply_one(service, "evt-1", "urn:other", data(version=1))
    assert world.store[UID].name == "New" and world.store[UID].version == 2


async def test_apply_event_newer_version_applied():
    world = FakeWorld()
    service = make_service(world)
    await apply_one(service, "evt-1", "urn:other", data(version=1))
    await apply_one(service, "evt-2", "urn:other", data(name="V5", version=5))
    assert world.store[UID].name == "V5" and world.store[UID].version == 5


# ------------------------------------------------- batched consumer path


async def test_apply_events_batch_single_transaction():


    world = FakeWorld()
    items = [
        UserEventItem(f"evt-{n}", "urn:other",
                      UserData(id=uuid.uuid4(), name=f"u{n}", email="e@x.com",
                               attributes={}, version=1))
        for n in range(10)
    ]
    await make_service(world).apply_state_events(items)
    assert len(world.store) == 10
    assert len(world.uows) == 1 and world.uows[0].committed  # one transaction
    assert world.publisher.published == []  # still never republishes


async def test_apply_events_batch_highest_version_wins_within_batch():


    world = FakeWorld()
    items = [
        UserEventItem("evt-1", "urn:other", data(name="old", version=1)),
        UserEventItem("evt-3", "urn:other", data(name="newest", version=3)),
        UserEventItem("evt-2", "urn:other", data(name="middle", version=2)),
    ]
    await make_service(world).apply_state_events(items)
    assert world.store[UID].name == "newest" and world.store[UID].version == 3


async def test_apply_events_batch_filters_duplicates_and_stale():


    world = FakeWorld()
    service = make_service(world)
    await apply_one(service, "evt-1", "urn:other", data(name="live", version=2))
    await service.apply_state_events([
        UserEventItem("evt-1", "urn:other", data(name="dup", version=9)),   # duplicate id
        UserEventItem("evt-0", "urn:other", data(name="stale", version=1)),  # stale
    ])
    assert world.store[UID].name == "live" and world.store[UID].version == 2
