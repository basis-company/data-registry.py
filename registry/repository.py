from dataclasses import dataclass
from functools import cache
from typing import Optional

from registry.drivers import Driver
from registry.entity import Bucket, Entity, Storage
from registry.schema import BucketStatus


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

        self.map: dict[type[Entity], dict[int, Entity]] = {}
        for entity in self.entities:
            self.indexes.insert(0, UniqueIndex(entity, ['id']))
            self.map[entity] = {}

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
        if row['id'] in self.map[entity]:
            instance = self.map[entity][row['id']]
            for key, value in row.items():
                setattr(instance, key, value)
        else:
            instance = entity(**{
                k: v for (k, v) in row.items() if k != 'bucket_id'
            })
            self.map[entity][row['id']] = instance

        return instance


def get_entity_repository_class(entity: type[Entity]) -> type[Repository]:
    map = get_entity_repository_map()
    if entity not in map:
        raise LookupError(f'No entity repository found: {entity}')
    return map[entity]


@cache
def get_entity_repository_map() -> dict[type[Entity], type[Repository]]:
    map: dict[type[Entity], type[Repository]] = {}
    for repository in Repository.__subclasses__():
        for entity in repository.entities:
            if entity in map:
                raise LookupError(f'Duplicate entity repository: {entity}')
            map[entity] = repository

    return map


class BucketRepository(Repository):
    bucket_id: int = 1
    entities = [Bucket]

    async def bootstrap(self, driver: Driver) -> None:
        bucket_row = await driver.find_or_create(
            entity=Bucket,
            query={'id': BucketRepository.bucket_id},
            data={
                'bucket_id': BucketRepository.bucket_id,
                'id': BucketRepository.bucket_id,
                'key': '',
                'repository': BucketRepository,
                'status': BucketStatus.READY,
                'storage_id': 1,
            }
        )

        storage_row = await driver.find_or_create(
            entity=Bucket,
            query={'id': StorageRepository.bucket_id},
            data={
                'bucket_id': BucketRepository.bucket_id,
                'id': StorageRepository.bucket_id,
                'key': '',
                'repository': StorageRepository,
                'status': BucketStatus.READY,
                'storage_id': 1,
            }
        )

        self.make(Bucket, bucket_row)
        self.make(Bucket, storage_row)


class StorageRepository(Repository):
    bucket_id: int = 2
    entities = [Storage]

    async def bootstrap(self, driver: Driver, storage: Storage) -> None:
        await driver.find_or_create(
            entity=Storage,
            query={'id': storage.id},
            data=dict(
                bucket_id=StorageRepository.bucket_id, **storage.__dict__
            ),
        )
