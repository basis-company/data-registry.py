from functools import cache
from typing import Optional, Protocol

from sharded.entity import Entity
from sharded.schema import StorageDriver


class Driver(Protocol):
    def __init__(self, dsn: str) -> None:
        ...

    async def create(self, name: str, data: dict) -> dict:
        raise NotImplementedError()

    async def find(self, name: str, queries: list[dict]) -> list[dict]:
        raise NotImplementedError()

    async def find_or_create(
        self, name: str, query: dict, data: dict
    ) -> dict:
        result = await self.find(name, [query])
        if len(result):
            return result[0]

        return await self.create(name, data)

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
    data: Optional[dict[str, list[dict]]] = None

    async def create(self, name: str, data: dict) -> dict:
        data['id'] = len(self.data[name]) + 1
        self.data[name].append(data)
        return data

    async def find(self, name: str, queries: list[dict]) -> list[dict]:
        return [
            row for row in self.data[name]
            if await self.is_valid(row, queries)
        ]

    async def init_schema(self, entity: Entity) -> None:
        if not self.data:
            self.data = {}

        if entity.__name__ not in self.data:
            self.data[entity.__name__] = []

    async def is_valid(self, row, queries: list) -> bool:
        for query in queries:
            if False not in [
                row[key] == value for (key, value) in query.items()
            ]:
                return True

        return False
