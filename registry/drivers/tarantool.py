from asyncio import create_task, gather
from decimal import Decimal
from math import ceil
from typing import Any, Callable, Optional, get_type_hints

from asynctnt import Connection
from asynctnt.iproto.protocol import SchemaIndex
from dsnparse import parse

from registry.drivers import Driver
from registry.entity import Entity
from registry.repository import Index, get_entity_repository_class

constructor_cache: dict[str, Callable] = {}


def get_constructor(
    space: str,
    connection: Connection
) -> Callable:
    if space not in constructor_cache:
        keys = list(
            connection.schema.spaces[space].metadata.name_id_map.keys()
        )
        source = [
            'def ' + space + '(source) -> dict:',
            '  return {',
        ] + [
            '    "' + keys[n] + '": source[' + str(n) + '],'
            for n in range(0, len(keys))
        ] + [
            '  }'
        ]
        exec("\n\r".join(source), constructor_cache)

    return constructor_cache[space]


class TarantoolDriver(Driver):
    page_size: int = 5000
    max_threads: int = 2

    def __init__(self, dsn: str) -> None:
        info = parse(dsn)
        self.connection = Connection(
            host=info.host,
            port=info.port,
            username=info.username or 'guest',
            password=info.password or '',
        )
        self.format: dict[type[Entity], dict[str, type]] = {}
        self.init: dict[type[Entity], bool] = {}

    async def find(
        self,
        entity: type[Entity],
        queries: list[dict],
        limit: Optional[int] = None,
    ) -> list[dict]:
        await self.init_schema(entity)
        constructor = get_constructor(entity.__name__, self.connection)
        keys = (queries[0] or {}).keys()
        index = cast_index(self.connection, entity.__name__, keys)
        params = [get_index_tuple(index, query) for query in queries]
        result: list[dict] = []

        async def worker():
            while len(params) > 0:
                values = [
                    params.pop()
                    for _ in range(min(self.page_size, len(params)))
                ]

                query = """
                local space, index, values = ...
                local result = {}
                for i, value in pairs(values) do
                    local rows = {}
                    box.space[space].index[index]:pairs(value)
                        :each(function(t)
                            table.insert(rows, t)
                        end)
                    if #rows > 0 then
                        table.insert(result, rows)
                    end
                end
                return result
                """

                query_params = (entity.__name__, index.name, values)
                eval = await self.connection.eval(query, query_params)
                for col in eval[0]:
                    result.extend([constructor(t) for t in col])

                if limit is not None and len(result) > limit:
                    break

        workers = 1
        if limit != 1:
            workers = min(self.max_threads, ceil(len(params) / self.page_size))

        tasks = [create_task(worker()) for _ in range(workers)]

        await gather(*tasks, return_exceptions=True)

        return result

    async def insert(self, entity: type[Entity], data: dict) -> dict:
        await self.init_schema(entity)

        lua = f'''
        local tuple = ...
        if tuple[2] == 0 then
            if box.sequence.sq_{entity.__name__} == nil then
                opts = {'{}'}
                last = box.space.{entity.__name__}.index.bucket_id_id:max()
                if last ~= nil then
                    opts['start'] = last.id + 1
                end
                box.schema.sequence.create('sq_{entity.__name__}', opts)
            end
            tuple[2] = box.sequence.sq_{entity.__name__}:next()
        end
        return box.space.{entity.__name__}:insert(tuple)
        '''

        res = await self.connection.eval(lua, [
            self.get_row_tuple(entity.__name__, data)
        ])

        return self.get_tuple_dict(entity.__name__, res[0])

    async def init_schema(self, entity: type[Entity]) -> None:
        if not self.connection.is_connected:
            await self.connection.connect()

        if entity not in self.init:
            self.init[entity] = True
            if entity.__name__ not in self.connection.schema.spaces:
                await self.connection.eval('box.schema.create_space(...)', [
                    entity.__name__, {
                        'engine': 'memtx',
                        'if_not_exists': True,
                    }
                ])
                await self.connection.refetch_schema()
            await sync_format(self.connection, entity)
            await sync_indexes(self.connection, entity)

    def get_tuple_dict(self, space, param):
        format = get_current_format(self.connection, space)
        return {
            format[n]['name']: param[n] for n in range(len(format))
        }

    def get_row_tuple(self, space, param):
        return [
            convert_type(field['type'], param[field['name']])
            if field['name'] in param else convert_type(field['type'], None)
            for field in get_current_format(self.connection, space)
        ]


def convert_type(tarantool_type: str, value: Any) -> str | int | float:
    if value is None or value == '':
        if tarantool_type == 'number' or tarantool_type == 'unsigned':
            return 0
        if tarantool_type == 'string' or tarantool_type == 'str':
            return value or ''

    if isinstance(value, str):
        match(tarantool_type):
            case 'unsigned':
                return int(value)

            case 'number':
                if value[-1] == '-':
                    return -1 * float(value[0:-1])
                else:
                    return float(value)

    if isinstance(value, int) and tarantool_type == 'string':
        return str(value)

    if isinstance(value, Decimal):
        if tarantool_type == 'number':
            return float(value)
        if tarantool_type == 'unsigned':
            return int(value)

    if isinstance(value, type):
        return str(value)

    return value


def get_index_tuple(index, param):
    return [
        convert_type(field.type, param[field.name])
        for field in index.metadata.fields
        if field.name in param
    ]


def cast_index(
    connection: Connection,
    space: str,
    keys: list[str]
) -> SchemaIndex:
    if not isinstance(keys, list):
        keys = list(keys)

    if not len(keys):
        return connection.schema.spaces[space].indexes[0]

    keys.sort()

    for index in connection.schema.spaces[space].indexes.values():
        candidate_keys = []
        for f in index.metadata.fields:
            candidate_keys.append(f.name)
            if len(candidate_keys) == len(keys):
                break

        candidate_keys.sort()
        if candidate_keys == keys:
            return index

    raise ValueError(f'no index on {space} for [{",".join(keys)}]')


async def sync_format(connection, entity) -> None:
    format = [{
        'name': 'bucket_id',
        'type': 'unsigned',
    }] + [
        {
            'name': key,
            'type': get_tarantool_type(type),
        }
        for (key, type) in get_type_hints(entity).items()
    ]

    if format != get_current_format(connection, entity.__name__):
        box_space = 'box.space.' + entity.__name__
        await connection.eval(box_space + ':format(...)', [format])
        await connection.refetch_schema()


def get_current_format(connection, space: str):
    if space not in connection.schema.spaces:
        raise LookupError(f'Invalid space {space}')

    if not connection.schema.spaces[space].metadata:
        return []

    return [
        {'name': field.name, 'type': field.type}
        for field in connection.schema.spaces[space].metadata.fields
    ]


async def sync_indexes(connection, entity) -> None:
    indexes = [Index(entity, ['bucket_id', 'id'], True)]
    indexes.extend([
        i for i in get_entity_repository_class(entity).indexes
        if i.entity == entity
    ])
    indexes.extend([
        Index(i.entity, ['bucket_id'] + i.fields, i.unique)
        for i in get_entity_repository_class(entity).indexes
        if i.entity == entity
    ])

    box_space = 'box.space.' + entity.__name__
    changes: bool = False
    for index in indexes:
        if index.name not in connection.schema.spaces[entity.__name__].indexes:
            changes = True
            await connection.eval(box_space + ':create_index(...)', [
                index.name, {'parts': index.fields, 'unique': index.unique}
            ])

    if changes:
        await connection.refetch_schema()


def get_tarantool_type(type) -> str:
    if type is float:
        return 'number'
    if type is int:
        return 'unsigned'
    return 'str'
