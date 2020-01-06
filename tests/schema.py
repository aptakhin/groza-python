from enum import Enum
from typing import List, Optional, Dict


class TType(Enum):
    INT8 = 1
    STR = 2
    BIGSERIAL = 3


class TColumn:
    def __init__(self, name: str, type_: TType):
        self.name = name
        self.type_ = type_


class TRow(dict):
    pass


class TTable:
    def __init__(self, name: str, columns: List[TColumn], data: Optional[List[TRow]]=None):
        self.name = name
        self.columns = columns
        self.data = data


class TSchema:
    def __init__(self, tables: List[TTable]):
        self.tables: Dict[str, TTable] = {t.name: t for t in tables}


class AsyncpgSchemaExecutor:
    @classmethod
    def _format_type(cls, type_):
        if type_ == TType.INT8:
            return "int8"
        elif type_ == TType.STR:
            return "text"
        elif type_ == TType.BIGSERIAL:
            return "text"
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

    @classmethod
    async def apply(cls, storage, schema: TSchema):
        async with storage.session() as session:
            async with session.transaction():
                for table in schema.tables.values():
                    q = f"CREATE TABLE {table.name} ("
                    for cnt, column in enumerate(table.columns):
                        q += f"{column.name} {cls._format_type(column.type_)} NOT NULL"
                        q += (", " if cnt < len(table.columns) - 1 else "")
                    q += ")"
                    await session.raw_conn().execute(q)

                for table in schema.tables.values():
                    for d in table.data:
                        q = f"INSERT INTO {table.name} ("
                        for cnt, column in enumerate(table.columns):
                            q += f'"{column.name}"'
                            q += (", " if cnt < len(table.columns) - 1 else "")
                        q += ") VALUES ("
                        for cnt, column in enumerate(table.columns):
                            q += f'{cls._format_value(d[column.name], column.type_)}'
                            q += (", " if cnt < len(table.columns) - 1 else "")
                        q += ")"
                        await session.raw_conn().execute(q)

    @staticmethod
    async def destroy(storage, schema: TSchema):
        async with storage.session() as session:
            async with session.transaction():
                for table in schema.tables.values():
                    q = f"DROP TABLE IF EXISTS {table.name}"
                    await session.raw_conn().execute(q)
