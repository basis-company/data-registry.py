from typing import Optional, Protocol

from registry.entity import Entity
from registry.schema import StorageDriver


class Driver(Protocol):
    def __init__(self, dsn: str) -> None:
        ...

    async def find(
        self,
        entity: type[Entity],
        queries: list[dict],
        limit: Optional[int] = None,
    ) -> list[dict]:
        raise NotImplementedError()

    async def find_one(
        self,
        entity: type[Entity],
        queries: list[dict],
    ) -> Optional[dict]:
        rows = await self.find(entity, queries, limit=1)
        if len(rows):
            return rows[0]

    async def find_or_create(
        self, entity: type[Entity], query: dict, data: dict
    ) -> dict:
        result = await self.find(entity, [query])
        if len(result):
            return result[0]

        return await self.insert(entity, data)

    async def find_or_fail(
        self,
        entity: type[Entity],
        queries: list[dict],
    ) -> dict:
        instance = await self.find_one(entity, queries)
        if not instance:
            raise LookupError(f'{entity.nick} not found')

        return instance

    async def init_schema(self, entity: type[Entity]) -> None:
        raise NotImplementedError()

    async def insert(self, entity: type[Entity], data: dict) -> dict:
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

    async def find(
        self,
        entity: type[Entity],
        queries: list[dict],
        limit: Optional[int] = None,
    ) -> list[dict]:
        await self.init_schema(entity)
        rows = [
            row for row in self.data[entity]
            if await self.is_valid(row, queries)
        ]
        if limit:
            rows = rows[0:limit]
        return rows

    async def init_schema(self, entity: type[Entity]) -> None:
        if entity not in self.data:
            self.data[entity] = []

    async def insert(self, entity: type[Entity], data: dict) -> dict:
        await self.init_schema(entity)
        data['id'] = len(self.data[entity]) + 1
        self.data[entity].append(data)
        return data

    async def is_valid(self, row, queries: list) -> bool:
        for query in queries:
            if False not in [
                row[key] == value for (key, value) in query.items()
            ]:
                return True

        return False
