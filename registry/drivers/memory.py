from typing import Optional

from registry.drivers import Driver
from registry.entity import Entity


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
