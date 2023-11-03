from dataclasses import dataclass
from functools import cache
from typing import Optional

from sharded.drivers import Driver
from sharded.entity import Bucket, Entity, Storage
from sharded.schema import BucketStatus


@dataclass
class Index:
    entity: type[Entity]
    fields: list[str]
    unique: bool = False


class UniqueIndex(Index):
    unique = True


class Repository:
    entities: list[type[Entity]]
    indexes: Optional[list[Index]] = None

    def __init__(self) -> None:
        if not self.indexes:
            self.indexes = []

        for entity in self.entities:
            self.indexes.insert(0, UniqueIndex(entity, ['id']))

    async def cast_storage(self, storages: list[Storage]) -> Storage:
        return storages[0]

    async def get_key(self, context: dict) -> str:
        return ''

    async def init_data(self, bucket: Bucket, driver: Driver) -> None:
        bucket.status = BucketStatus.READY

    async def init_schema(self, driver: Driver) -> None:
        for entity in self.entities:
            await driver.init_schema(entity)

    def make(self, entity: type[Entity], row: dict) -> Entity:
        return entity(**{k: v for (k, v) in row.items() if k != 'bucket_id'})


@cache
def get_entity_repository_class(entity: type[Entity]) -> type[Repository]:
    repositories = [
        repository for repository in Repository.__subclasses__()
        if entity in repository.entities
    ]
    if not len(repositories):
        raise LookupError(f'No entity repository found: {entity}')

    if len(repositories) > 1:
        raise LookupError(f'Duplicate entity repository: {entity}')

    return repositories[0]


class BucketRepository(Repository):
    buckets: dict[type[Entity], Bucket]
    entities = [Bucket]

    def __init__(self) -> None:
        self.buckets = {}

    async def bootstrap(self, driver: Driver) -> tuple[Bucket, Bucket]:
        bucket_row = await driver.find_or_create(
            name='Bucket',
            query={'id': 1},
            data={
                'bucket_id': 1,
                'id': 1,
                'key': '',
                'repository': BucketRepository,
                'status': BucketStatus.READY,
                'storage_id': 1,
            }
        )

        storage_row = await driver.find_or_create(
            name='Bucket',
            query={'id': 2},
            data={
                'bucket_id': 1,
                'id': 2,
                'key': '',
                'repository': StorageRepository,
                'status': BucketStatus.READY,
                'storage_id': 1,
            }
        )

        self.buckets[Bucket] = self.make(Bucket, bucket_row)
        self.buckets[Storage] = self.make(Bucket, storage_row)


class StorageRepository(Repository):
    entities = [Storage]

    async def bootstrap(self, driver: Driver, storage: Storage):
        await driver.find_or_create(
            name='Storage',
            query={'id': storage.id},
            data=dict(bucket_id=2, **storage.__dict__),
        )
