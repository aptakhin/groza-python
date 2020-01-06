import asyncio

import pytest

from groza import GrozaUser
from groza.instance import Groza
from groza.storage import GrozaVisor, groza_db, GrozaVisors, groza_visors

from tests.memory_storage import MemoryStorage


def test_fetch_sub():
    data = {
        "accounts": [
            {"id": 1, "name": "aaa"},
            {"id": 2, "name": "bbb"},
        ]
    }

    # TODO: to context init
    groza_db.set(MemoryStorage(data))
    groza_visors.set(GrozaVisors())

    class Account(GrozaVisor):
        table = "accounts"
        primary_key = "id"

    groza = Groza()
    subscription = {
        "allAccounts": {"visor": "Account"},
    }
    resp = asyncio.get_event_loop().run_until_complete(groza.fetch_sub(GrozaUser(), subscription))
    data = resp.data["data"]
    sub = resp.data["sub"]

    assert set(data["accounts"].keys()) == {1, 2}

    assert sub["allAccounts"]["status"] == "ok"
    assert set(sub["allAccounts"]["ids"]) == {1, 2}


def test_insert():
    data = {
        "accounts": [
            {"id": 1, "name": "aaa"},
            {"id": 2, "name": "bbb"},
        ]
    }

    # TODO: to context init
    groza_db.set(MemoryStorage(data))
    groza_visors.set(GrozaVisors())

    class Account(GrozaVisor):
        table = "accounts"
        primary_key = "id"

    groza = Groza()
    query = {
        "visor": "Account",
    }
    insert = {
        "name": "ccc",
    }
    resp = asyncio.get_event_loop().run_until_complete(groza.query_insert(user=GrozaUser(), query=query, insert=insert))
    assert resp.data["id"] == 3


def test_update():
    data = {
        "accounts": [
            {"id": 1, "name": "aaa"},
            {"id": 2, "name": "bbb"},
        ]
    }

    # TODO: to context init
    groza_db.set(MemoryStorage(data))
    groza_visors.set(GrozaVisors())

    class Account(GrozaVisor):
        table = "accounts"
        primary_key = "id"

    groza = Groza()
    update = [
        [{"visor": "Account", "id": 1}, {"name": "aaa1"}],
    ]
    resp = asyncio.get_event_loop().run_until_complete(groza.query_update(user=GrozaUser(), update=update))
    assert resp.data["status"] == "ok"

    assert data["accounts"][0]["id"] == 1
    assert data["accounts"][0]["name"] == "aaa1"


if __name__ == "__main__":
    pytest.main(["-s", "-x", __file__])
