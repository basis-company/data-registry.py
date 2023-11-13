from dataclasses import dataclass

from registry.schema import BucketStatus, StorageClass, StorageDriver


@dataclass
class Entity:
    id: int


@dataclass
class Storage(Entity):
    storage_class: StorageClass
    driver: StorageDriver
    dsn: str

    def __hash__(self) -> int:
        return hash(self.id)


@dataclass
class Bucket(Entity):
    key: str
    repository: str
    storage_id: int
    status: BucketStatus

    def __hash__(self) -> int:
        return hash((self.repository, self.key))
