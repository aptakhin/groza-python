from abc import abstractmethod

from groza import GrozaUser
from groza.storage import GrozaStorage, GrozaSession, GrozaVisor, GrozaInput


class _MemoryConn:
    def __init__(self, data):
        self._data = data


class _MemoryTransactionProxy:
    def __init__(self, data):
        self._data = data

    async def __aenter__(self):
        return _MemoryConn(data=self._data)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class MemorySession(GrozaSession):
    def __init__(self, data):
        self._data = data

    def transaction(self):
        return _MemoryTransactionProxy(self._data)

    def raw_conn(self):
        pass

    async def insert(self, *, visor: GrozaVisor, insert: GrozaInput, user: GrozaUser):
        visor_data = self._data.tables[visor.table].data

        insert_data = {**insert}
        insert_data[visor.primary_key] = max(d["id"] for d in visor_data) + 1 if visor_data else 1
        self._data.tables[visor.table].data.append(insert_data)
        return insert_data

    async def update(self, *, visor: GrozaVisor, update, user: GrozaUser):
        visor_data = self._data.tables[visor.table].data
        query, upd = update
        search = [cnt for cnt, d in enumerate(visor_data) if d[visor.primary_key] == query[visor.primary_key]]
        if not search:
            return
        idx = search[0]
        visor_data[idx].update(upd)

    async def delete(self, *, visor: "GrozaVisor", delete, user: GrozaUser):
        pass

    async def query(self, *, visor: GrozaVisor, from_sub: dict, all_sub: dict, sub_resp: dict, where=None, order=None):
        visor_data = self._data.tables[visor.table].data

        add_data = {}
        for item in visor_data:
            add_data[item[visor.primary_key]] = item

        return add_data, ""


class _MemorySessionProxy:
    def __init__(self, data):
        self._data = data

    async def __aenter__(self):
        return MemorySession(data=self._data)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class MemoryStorage(GrozaStorage):
    def __init__(self, data=None):
        self._data = data or {}

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        self._data = data

    def release(self, session: MemorySession):
        pass

    def session(self) -> _MemorySessionProxy:
        return _MemorySessionProxy(data=self._data)
