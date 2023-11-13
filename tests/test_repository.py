from dataclasses import dataclass

from pytest import mark

from registry.entity import Entity
from registry.registry import Registry
from registry.repository import Index, Repository


@dataclass
class Action(Entity):
    type: str


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
async def test_hello():
    registry = Registry()
    assert len(await registry.find(Action)) == 0
    action1 = await registry.find_or_create(Action, {'type': 'tester'})
    action2 = await registry.find_or_create(Action, {'type': 'tester2'})
    assert action1.id == 1
    assert action1.type == 'tester'
    assert action2.id == 2
    assert action2.type == 'tester2'
    assert len(await registry.find(Action)) == 2
    assert (await registry.get_instance(Action, 2)).type == 'tester2'
    assert (await registry.get_instance(Action, 3)) is None
