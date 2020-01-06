from enum import Enum
from typing import List, Optional


class TType(Enum):
    INT8 = 1
    STR = 2


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
        self.tables = {t.name: t for t in tables}


class AsyncpgSchemaExecutor:
    @staticmethod
    def apply(self, storage, schema: TSchema):
        pass
