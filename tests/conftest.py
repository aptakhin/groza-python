import asyncio
import os
from operator import itemgetter

import pytest

from groza.queue.asyncio_queue import AsyncioQueue
from groza.storage import groza_db, groza_visors, GrozaVisors
from groza.storage.asyncpg import AsyncpgStorage
from tests.storage.memory import MemoryStorage
from tests.schema import TSchema
from tests.schema.asyncpg import AsyncpgSchemaExecutor


class PytestMemoryStorage:
    """
    Pytest storage wrapper around memory storage
    """

    def __init__(self):
        groza_db.set(MemoryStorage())
        groza_visors.set(GrozaVisors())

    def setup(self, schema: TSchema):
        q = groza_db.get()
        q.schema = schema

    def destroy(self):
        pass

    @property
    def _schema(self) -> TSchema:
        db = groza_db.get()
        return db.schema

    def query(self, table_name, order_field):
        schema = self._schema
        return sorted(schema.tables[table_name].data, key=itemgetter(order_field))


class PytestAsyncpgStorage:
    """
    Applies Postgres storage tables to test database.
    """

    def __init__(self):
        self._notifications = AsyncioQueue()

        postgres_test_dsn = os.getenv('POLAR_SITE_BE_TEST_POSTGRES_DSN')
        self._storage = AsyncpgStorage(postgres_test_dsn)
        asyncio.get_event_loop().run_until_complete(
            self._storage.install(self._notifications))

        self._schema = None
        self._schema_exec = None

    def setup(self, schema: TSchema):
        self._schema = schema
        self._schema_exec = AsyncpgSchemaExecutor(self._storage, self._schema)

        asyncio.get_event_loop().run_until_complete(self._start_async())

        groza_db.set(self._storage)
        groza_visors.set(GrozaVisors())

    async def _start_async(self):
        await self._storage.connect()
        await self._schema_exec.apply()
        await self._storage.start_tables()

    def destroy(self):
        asyncio.get_event_loop().run_until_complete(self._schema_exec.destroy())

    def query(self, table_name, order_field):
        return asyncio.get_event_loop().run_until_complete(self._schema_exec.query(table_name, order_field))


@pytest.fixture(scope='function', params=[PytestMemoryStorage, PytestAsyncpgStorage])
def groza_storage(request):
    groza_storage = request.param()

    yield groza_storage

    groza_storage.destroy()
