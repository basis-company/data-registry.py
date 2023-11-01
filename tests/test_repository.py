from dataclasses import dataclass

from pytest import mark

from sharded.entity import Entity
from sharded.gateway import Gateway
from sharded.repository import Index, Repository


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
    gateway = Gateway()
    assert len(await gateway.find(Action)) == 0
    action1 = await gateway.find_or_create(Action, {'type': 'tester'})
    action2 = await gateway.find_or_create(Action, {'type': 'tester2'})
    assert action1.id == 1
    assert action1.type == 'tester'
    assert action2.id == 2
    assert action2.type == 'tester2'
    assert len(await gateway.find(Action)) == 2
    assert (await gateway.get(Action, 2)).type == 'tester2'
    assert (await gateway.get(Action, 3)) is None
