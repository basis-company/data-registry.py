from dataclasses import dataclass
from functools import cache
from typing import Optional, TypeVar

from sharded.drivers import Driver, get_driver
from sharded.entity import Bucket, Entity, Storage
from sharded.repository import (BucketRepository, Repository,
                                     StorageRepository,
                                     get_entity_repository_class)
from sharded.schema import BucketStatus, StorageClass, StorageDriver


@dataclass
class QueryContext:
    bucket: Bucket
    driver: Driver
    entity: Entity
    repository: Repository


class Gateway:
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

    async def bootstrap(self, source: Optional[Storage] = None):
        if self.ready:
            return

        self.ready = True
        if not source:
            source = self.storages[0]

        driver = await get_driver(source.driver, source.dsn)
        await driver.init_schema(Bucket)
        await driver.init_schema(Storage)
        await self.repositories[BucketRepository].bootstrap(driver)
        await self.repositories[StorageRepository].bootstrap(driver, source)

    async def find_or_create(
        self,
        entity: type[Entity],
        create: Optional[dict | list] = None,
        find: Optional[dict | list] = None,
        key: Optional[any] = None,
    ) -> Entity:
        if find is None:
            find = create
        context = await self.context(entity, key)
        return context.repository.make(
            entity=entity,
            row=await context.driver.find_or_create(
                name=entity.__name__,
                find=dict(bucket_id=context.bucket.id, **find),
                create=dict(bucket_id=context.bucket.id, **create),
            )
        )

    async def find(
        self,
        entity: type[Entity],
        params: Optional[dict | list] = None,
        key: Optional[any] = None,
    ) -> list[Entity]:
        # return repository.get_instances(entity, data)
        # return await repository.find(entity, bucket, params)
        context = await self.context(entity, key)
        if not params:
            params = {}
        if isinstance(params, dict):
            params = [params]

        params = [
            dict(bucket_id=context.bucket.id, **param) for param in params
        ]
        rows = await context.driver.find(entity.__name__, params)

        return [context.repository.make(entity, row) for row in rows]

    async def get(
        self,
        entity: type[Entity],
        id: int,
        key: Optional[any] = None,
    ) -> Optional[Entity]:
        instances = await self.find(entity, {'id': id}, key)
        if len(instances):
            return instances[0]

    async def context(self, entity: type[Entity], key: any) -> QueryContext:
        await self.bootstrap()

        repository = self.repositories[get_entity_repository_class(entity)]
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

    async def get_bucket(self, repository: Repository, key: any) -> Bucket:
        if isinstance(repository, BucketRepository):
            return self.repositories[BucketRepository].bucket_bucket
        if isinstance(repository, StorageRepository):
            return self.repositories[BucketRepository].storage_bucket
        return await self.find_or_create(
            entity=Bucket,
            find={
                'key': await repository.get_key(key),
                'repository': repository,
            },
            create={
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

        if not Storage:
            raise LookupError(
                f'storage {storage_id} not found'
            )

        return storage
