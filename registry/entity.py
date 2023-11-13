from dataclasses import _MISSING_TYPE, dataclass

from registry.schema import BucketStatus, StorageClass, StorageDriver


@dataclass
class Entity:
    id: int

    @classmethod
    def get_default_values(cls) -> dict:
        return {
            key: field.default
            for key, field in cls.__dataclass_fields__.items()
            if not isinstance(field.default, _MISSING_TYPE)
        }


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
    status: BucketStatus
    storage_id: int = 0

    def __hash__(self) -> int:
        return hash((self.repository, self.key))
