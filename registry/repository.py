from dataclasses import dataclass
from functools import cache
from typing import Any, Optional

from registry.drivers import Driver
from registry.entity import Bucket, Entity, Storage
from registry.schema import BucketStatus


@dataclass
class Index:
    entity: type[Entity]
    fields: list[str]
    unique: bool = False

    @property
    def name(self) -> str:
        return '_'.join(self.fields)


class UniqueIndex(Index):
    unique = True


class Repository:
    entities: list[type[Entity]]
    indexes: list[Index] = []

    def __init__(self) -> None:
        self.map: dict[type[Entity], dict[int, Entity]] = {}
        for entity in self.entities:
            self.map[entity] = {}

    async def cast_storage(self, storages: list[Storage]) -> Storage:
        return storages[0]

    async def transform_key(self, key: Optional[Any] = None) -> str:
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
    indexes: list[Index] = [
        Index(entity=Bucket, fields=['repository', 'key'], unique=True)
    ]

    async def bootstrap(self, driver: Driver) -> None:
        bucket_row = await driver.find_or_create(
            entity=Bucket,
            query={
                'bucket_id': BucketRepository.bucket_id,
                'id': BucketRepository.bucket_id,
            },
            data={
                'bucket_id': BucketRepository.bucket_id,
                'id': BucketRepository.bucket_id,
                'key': '',
                'repository': BucketRepository,
                'status': BucketStatus.READY,
                'storage_id': StorageRepository.storage_id,
            }
        )

        storage_row = await driver.find_or_create(
            entity=Bucket,
            query={
                'bucket_id': BucketRepository.bucket_id,
                'id': StorageRepository.bucket_id,
            },
            data={
                'bucket_id': BucketRepository.bucket_id,
                'id': StorageRepository.bucket_id,
                'key': '',
                'repository': StorageRepository,
                'status': BucketStatus.READY,
                'storage_id': StorageRepository.storage_id,
            }
        )

        self.make(Bucket, bucket_row)
        self.make(Bucket, storage_row)


class StorageRepository(Repository):
    bucket_id: int = 2
    storage_id: int = 1
    entities = [Storage]

    async def bootstrap(self, driver: Driver, storage: Storage) -> None:
        if storage.id != self.storage_id:
            raise ValueError(f'Invalid storage_id: {storage.id}')
        await driver.find_or_create(
            entity=Storage,
            query=dict(
                bucket_id=StorageRepository.bucket_id,
                id=storage.id,
            ),
            data=dict(
                bucket_id=StorageRepository.bucket_id,
                **storage.__dict__
            ),
        )
