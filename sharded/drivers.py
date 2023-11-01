from functools import cache
from typing import Protocol
from sharded.entity import Entity

from sharded.schema import StorageDriver


class Driver(Protocol):
    def __init__(self, dsn: str) -> None:
        ...

    async def find(self, name: str, params: dict) -> list[dict]:
        raise NotImplementedError()

    async def find_or_create(
        self, name: str, find: dict, create: dict
    ) -> dict:
        raise NotImplementedError()

    async def init_schema(self, entity: Entity) -> None:
        raise NotImplementedError()


driver_instances: dict[str, dict[str, Driver]] = {}


async def get_driver(driver: StorageDriver, dsn: str) -> Driver:
    if driver not in driver_instances:
        driver_instances[driver] = {}
    if dsn not in driver_instances[driver]:
        driver_instances[driver][dsn] = get_implementation(driver)(dsn)
    return driver_instances[driver][dsn]


@cache
def get_implementation(driver):
    implementations: dict[StorageDriver, Driver] = {
        StorageDriver.MEMORY: MemoryDriver
    }
    if driver in implementations:
        return implementations[driver]
    raise NotImplementedError(f'Driver {driver} not implemented')


class MemoryDriver(Driver):
    def __init__(self, dsn: str) -> None:
        super().__init__(dsn)
        self.data: dict[str, list[dict]] = {}

    async def find_or_create(
        self, name: str, find: dict, create: dict
    ) -> dict:
        rows = await self.find(name, [find])
        if len(rows):
            return rows[0]

        create['id'] = len(self.data[name]) + 1
        self.data[name].append(create)
        return create

    async def find(self, name: str, params: list) -> list[dict]:
        return [
            row for row in self.data[name]
            if await self.is_valid(row, params)
        ]

    async def is_valid(self, row, params) -> bool:
        for param in params:
            if False not in [row[key] == value for (key, value) in param.items()]:
                return True

        return False

    async def init_schema(self, entity: Entity) -> None:
        if entity.__name__ not in self.data:
            self.data[entity.__name__] = []
