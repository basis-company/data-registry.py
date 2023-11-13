from enum import StrEnum


class BucketStatus(StrEnum):
    NEW = 'n'
    SCHEMA = 's'
    READY = 'r'


class StorageClass(StrEnum):
    COLD = 'cold'
    HOT = 'hot'
    ICE = 'ice'
    MEMORY = 'memory'


class StorageDriver(StrEnum):
    MEMORY = 'memory'
    TARANTOOL = 'tarantool'
