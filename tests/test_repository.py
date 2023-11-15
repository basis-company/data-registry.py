from dataclasses import dataclass

from pytest import mark

from registry.drivers import get_driver
from registry.entity import Entity, Storage
from registry.registry import Registry
from registry.repository import Index, Repository
from registry.schema import StorageClass, StorageDriver


@dataclass
class Action(Entity):
    type: str
    owner_id: int = 0


@dataclass
class ActionTrigger(Entity):
    ...


class ActionRepository(Repository):
    entities = [
        Action,
        ActionTrigger
    ]
    indexes = [
        Index(ActionTrigger, ['id'])
    ]


@mark.asyncio
@mark.parametrize("storage", [
    Storage(1, StorageClass.HOT, StorageDriver.MEMORY, '1'),
    Storage(1, StorageClass.HOT, StorageDriver.MEMORY, '2'),
])
async def test_hello(storage: Storage):
    registry = Registry([storage])
    assert len(await registry.find(Action)) == 0

    # create two actions
    action1 = await registry.find_or_create(Action, {'type': 'tester'})
    action2 = await registry.find_or_create(Action, {'type': 'tester2'})

    # validate properties
    assert action1.id == 1
    assert action1.type == 'tester'
    assert action1.owner_id == 0
    assert action2.id == 2
    assert action2.type == 'tester2'
    assert action2.owner_id == 0

    # identity map
    action3 = await registry.find_or_create(Action, {'id': 2})
    assert action3 == action2
    action4 = await registry.find_or_create(Action, {'type': 'tester'})
    assert action4 == action1
    assert len(await registry.find(Action)) == 2

    # lookup checks
    assert (await registry.get_instance(Action, 2)).type == 'tester2'
    assert (await registry.get_instance(Action, 3)) is None

    # storage level persistence check
    [storage] = registry.storages
    driver = await get_driver(storage.driver, storage.dsn)
    assert len(await driver.find(Action, queries=[{}])) == 2

    # default values peristence
    first_action_dict = await driver.find_one(Action, queries=[{'id': 1}])
    assert first_action_dict['owner_id'] == 0
