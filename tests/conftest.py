import asyncio
import os

import pytest

from groza.storage import groza_db, groza_visors, GrozaVisors
from groza.storage.asyncpg import AsyncpgStorage
from tests.memory_storage import MemoryStorage
from tests.schema import TSchema


class PytestMemoryStorage:
    def __init__(self):
        groza_db.set(MemoryStorage())
        groza_visors.set(GrozaVisors())

    def setup(self, data: TSchema):
        q = groza_db.get()
        q.data = data

    # def storage(self):


class PytestAsyncpgStorage:
    def __init__(self):
        self._notifications = asyncio.Queue()

        pg_test_dsn = os.getenv("POLAR_SITE_BE_TEST_POSTGRES_DSN")

        self._storage = AsyncpgStorage(pg_test_dsn, self._notifications)
        asyncio.get_event_loop().run_until_complete(self._storage.connect())
        asyncio.get_event_loop().run_until_complete(self._storage.start_tables())

        groza_db.set(self._storage)
        groza_visors.set(GrozaVisors())

    def setup(self, data: TSchema):
        pass


@pytest.fixture(scope="module", params=[PytestMemoryStorage])
def groza_storage(request):
    groza_storage = request.param()
    print("Z", groza_storage)

    yield groza_storage
