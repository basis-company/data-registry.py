from functools import cache
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

        return None

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
            raise LookupError(f'{entity.__name__} not found')

        return instance

    async def init_schema(self, entity: type[Entity]) -> None:
        raise NotImplementedError()

    async def insert(self, entity: type[Entity], data: dict) -> dict:
        raise NotImplementedError()


@cache
def get_driver(driver: StorageDriver, dsn: str) -> Driver:
    return get_implementation(driver)(dsn)


@cache
def get_implementation(driver: StorageDriver) -> type[Driver]:
    if driver is StorageDriver.MEMORY:
        from registry.drivers.memory import MemoryDriver
        return MemoryDriver

    if driver is StorageDriver.TARANTOOL:
        from registry.drivers.tarantool import TarantoolDriver
        return TarantoolDriver

    raise NotImplementedError(f'{driver} driver not implemented')
