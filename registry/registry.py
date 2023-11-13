from dataclasses import dataclass
from functools import cache
from typing import Any, Optional

from registry.drivers import Driver, get_driver
from registry.entity import Bucket, Entity, Storage
from registry.repository import (BucketRepository, Repository,
                                 StorageRepository,
                                 get_entity_repository_class)
from registry.schema import BucketStatus, StorageClass, StorageDriver


@dataclass
class QueryContext:
    bucket: Bucket
    driver: Driver
    entity: type[Entity]
    repository: Repository


class Registry:
    def __init__(self) -> None:
        self.ready: bool = False
        self.repositories: dict[type[Repository], Repository] = {
            repository: repository()
            for repository in Repository.__subclasses__()
        }
        self.storages: list[Storage] = [
            Storage(
                id=1,
                storage_class=StorageClass.MEMORY,
                driver=StorageDriver.MEMORY,
                dsn=''
            )
        ]

    def get_repository(self, cls: type[Repository]) -> Repository:
        if cls not in self.repositories:
            raise LookupError(f'repository {cls} is not registered')

        return self.repositories[cls]

    async def bootstrap(self, source: Optional[Storage] = None):
        if self.ready:
            return

        self.ready = True
        if not source:
            source = self.storages[0]

        driver = await get_driver(source.driver, source.dsn)

        for repository in self.repositories.values():
            if isinstance(repository, BucketRepository):
                await repository.bootstrap(driver)
            if isinstance(repository, StorageRepository):
                await repository.bootstrap(driver, source)

    async def find_or_create(
        self,
        entity: type[Entity],
        data: Optional[dict] = None,
        query: Optional[dict] = None,
        key: Optional[Any] = None,
    ) -> Entity:
        if data is None:
            data = {}
        if query is None:
            query = data
        context = await self.context(entity, key)
        return context.repository.make(
            entity=entity,
            row=await context.driver.find_or_create(
                entity=entity,
                query=dict(bucket_id=context.bucket.id, **query),
                data=dict(bucket_id=context.bucket.id, **data),
            )
        )

    async def create(
        self,
        entity: type[Entity],
        data: dict,
        key: Optional[Any] = None,
    ) -> Entity:
        context = await self.context(entity, key)
        return context.repository.make(
            entity=entity,
            row=await context.driver.create(
                entity=entity,
                data=dict(bucket_id=context.bucket.id, **data),
            )
        )

    async def find(
        self,
        entity: type[Entity],
        queries: Optional[dict | list] = None,
        key: Optional[Any] = None,
    ) -> list[Entity]:
        if not queries:
            queries = {}
        if isinstance(queries, dict):
            queries = [queries]

        context = await self.context(entity, key)
        bucket_queries = [
            dict(bucket_id=context.bucket.id, **query)
            for query in queries
        ]

        rows = await context.driver.find(entity, bucket_queries)
        return [context.repository.make(entity, row) for row in rows]

    async def get_instance(
        self,
        entity: type[Entity],
        id: int,
        key: Optional[Any] = None,
    ) -> Optional[Entity]:
        instances = await self.find(entity, {'id': id}, key)
        if len(instances):
            return instances[0]

        return None

    async def context(self, entity: type[Entity], key: Any) -> QueryContext:
        await self.bootstrap()

        repository = self.get_repository(get_entity_repository_class(entity))
        bucket = await self.get_bucket(repository, key)

        if not bucket.storage_id:
            storage = await repository.cast_storage(self.storages)
            bucket.storage_id = storage.id
        else:
            storage = self.get_storage(bucket.storage_id)

        driver = await get_driver(storage.driver, storage.dsn)

        if bucket.status == BucketStatus.NEW:
            await repository.init_schema(driver)
            bucket.status = BucketStatus.SCHEMA
            # await self.persist(bucket)

        if bucket.status == BucketStatus.SCHEMA:
            await repository.init_data(bucket, driver)
            # await self.persist(bucket)

        if bucket.status != BucketStatus.READY:
            raise LookupError(f'Invalid status: {bucket.status}')

        return QueryContext(bucket, driver, entity, repository)

    async def get_bucket(self, repository: Repository, key: Any):
        if isinstance(repository, BucketRepository | StorageRepository):
            buckets = self.get_repository(BucketRepository)
            bucket = buckets.map[Bucket][repository.bucket_id]
            if isinstance(bucket, Bucket):
                return bucket

        return await self.find_or_create(
            entity=Bucket,
            query={
                'key': await repository.get_key(key),
                'repository': repository,
            },
            data={
                'key': await repository.get_key(key),
                'repository': repository,
                'status': BucketStatus.NEW,
                'storage_id': 0,
            },
        )

    @cache
    def get_storage(self, storage_id: int) -> Storage:
        storage = None
        for candidate in self.storages:
            if candidate.id == storage_id:
                storage = candidate

        if not storage:
            raise LookupError(
                f'storage {storage_id} not found'
            )

        return storage
