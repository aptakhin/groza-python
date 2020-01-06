import asyncio
import os

import pytest

from groza.storage import groza_db, groza_visors, GrozaVisors
from groza.storage.asyncpg import AsyncpgStorage
from tests.memory_storage import MemoryStorage
from tests.schema import TSchema, AsyncpgSchemaExecutor


class PytestMemoryStorage:
    def __init__(self):
        groza_db.set(MemoryStorage())
        groza_visors.set(GrozaVisors())

    def setup(self, data: TSchema):
        q = groza_db.get()
        q.data = data

    def destroy(self):
        pass


class PytestAsyncpgStorage:
    def __init__(self):
        self._notifications = asyncio.Queue()

        pg_test_dsn = os.getenv("POLAR_SITE_BE_TEST_POSTGRES_DSN")

        self._storage = AsyncpgStorage(pg_test_dsn, self._notifications)
        self._schema = None

    def setup(self, schema: TSchema):
        self._schema = schema
        asyncio.get_event_loop().run_until_complete(self._start_async())

        groza_db.set(self._storage)
        groza_visors.set(GrozaVisors())

    async def _start_async(self):
        await self._storage.connect()
        await AsyncpgSchemaExecutor.apply(self._storage, self._schema)
        await self._storage.start_tables()

    def destroy(self):
        print("Cleaning up")
        asyncio.get_event_loop().run_until_complete(AsyncpgSchemaExecutor.destroy(self._storage, self._schema))


@pytest.fixture(scope="function", params=[PytestMemoryStorage, PytestAsyncpgStorage])
def groza_storage(request):
    groza_storage = request.param()
    print("Z", groza_storage)

    yield groza_storage

    groza_storage.destroy()
