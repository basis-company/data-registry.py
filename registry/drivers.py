from typing import Protocol

from registry.entity import Entity
from registry.schema import StorageDriver


class Driver(Protocol):
    def __init__(self, dsn: str) -> None:
        ...

    async def create(self, entity: type[Entity], data: dict) -> dict:
        raise NotImplementedError()

    async def find(
        self, entity: type[Entity], queries: list[dict]
    ) -> list[dict]:
        raise NotImplementedError()

    async def find_or_create(
        self, entity: type[Entity], query: dict, data: dict
    ) -> dict:
        result = await self.find(entity, [query])
        if len(result):
            return result[0]

        return await self.create(entity, data)

    async def init_schema(self, entity: type[Entity]) -> None:
        raise NotImplementedError()


driver_instances: dict[str, dict[str, Driver]] = {}


async def get_driver(driver: StorageDriver, dsn: str) -> Driver:
    if driver not in driver_instances:
        driver_instances[driver] = {}
    if dsn not in driver_instances[driver]:
        driver_instances[driver][dsn] = get_implementation(driver)(dsn)
    return driver_instances[driver][dsn]


def get_implementation(driver: StorageDriver) -> type[Driver]:
    implementations: dict[StorageDriver, type[Driver]] = {
        StorageDriver.MEMORY: MemoryDriver
    }
    if driver in implementations:
        return implementations[driver]
    raise NotImplementedError(f'Driver {driver} not implemented')


class MemoryDriver(Driver):
    def __init__(self, dsn: str) -> None:
        self.data: dict[type[Entity], list[dict]] = {}

    async def create(self, entity: type[Entity], data: dict) -> dict:
        await self.init_schema(entity)
        data['id'] = len(self.data[entity]) + 1
        self.data[entity].append(data)
        return data

    async def find(
        self, entity: type[Entity], queries: list[dict]
    ) -> list[dict]:
        await self.init_schema(entity)
        return [
            row for row in self.data[entity]
            if await self.is_valid(row, queries)
        ]

    async def init_schema(self, entity: type[Entity]) -> None:
        if entity not in self.data:
            self.data[entity] = []

    async def is_valid(self, row, queries: list) -> bool:
        for query in queries:
            if False not in [
                row[key] == value for (key, value) in query.items()
            ]:
                return True

        return False
