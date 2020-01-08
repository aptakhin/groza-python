from enum import Enum
from typing import List, Optional, Dict

"""
Schema made for stable testing of Groza on different storages.
"""

class TType(Enum):
    INT8 = 1
    STR = 2
    BIGSERIAL = 3

    @property
    def is_serial(self):
        return self.value in (self.BIGSERIAL.value,)


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
