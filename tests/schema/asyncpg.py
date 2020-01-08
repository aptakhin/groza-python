from groza.storage.asyncpg import AsyncpgStorage, AsyncpgSession
from pssq import Q
from tests.schema import TSchema, TType


class AsyncpgSchemaExecutor:
    def __init__(self, storage: AsyncpgStorage, schema: TSchema):
        self._storage = storage
        self._schema = schema

    async def apply(self):
        async with self._storage.session() as session:
            async with session.transaction():
                await self._apply(session)

    async def destroy(self):
        async with self._storage.session() as session:
            async with session.transaction():
                for table in self._schema.tables.values():
                    q = f"DROP TABLE IF EXISTS {table.name}"
                    await session.raw_conn().execute(q)

    async def query(self, table_name, order_field):
        async with self._storage.session() as session:
            async with session.transaction():
                table = self._schema.tables[table_name]
                result = await (session.raw_conn().fetch(Q.select().
                                 from_(table.name).order(order_field, 1)))
                return [dict(res) for res in result]

    @classmethod
    def _format_type(cls, type_):
        if type_ == TType.INT8:
            return "int8"
        elif type_ == TType.STR:
            return "text"
        elif type_ == TType.BIGSERIAL:
            return "bigserial"
        else:
            raise RuntimeError("Unsupported type: %s" % type_)

    @classmethod
    def _format_value(cls, value, type_):
        if type_ in (TType.INT8, TType.BIGSERIAL):
            return value
        elif type_ == TType.STR:
            return f"'{value}'"
        else:
            raise RuntimeError("Unsupported type: %s" % type_)

    async def _apply(self, session: AsyncpgSession):
        for table in self._schema.tables.values():
            q = f"CREATE TABLE {table.name} ("
            for cnt, column in enumerate(table.columns):
                q += f"{column.name} {self._format_type(column.type_)} NOT NULL"
                q += (", " if cnt < len(table.columns) - 1 else "")
            q += ")"
            await session.raw_conn().execute(q)

        for table in self._schema.tables.values():
            for d in table.data:
                q = f"INSERT INTO {table.name} ("
                for cnt, column in enumerate(table.columns):
                    if column.type_.is_serial:
                        continue
                    q += f'"{column.name}"'
                    q += (", " if cnt < len(table.columns) - 1 else "")
                q += ") VALUES ("
                for cnt, column in enumerate(table.columns):
                    if column.type_.is_serial:
                        continue
                    q += f'{self._format_value(d[column.name], column.type_)}'
                    q += (", " if cnt < len(table.columns) - 1 else "")
                q += ")"
                await session.raw_conn().execute(q)
