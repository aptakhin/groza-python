import asyncio

import pytest

from groza import GrozaUser
from groza.instance import Groza
from groza.storage import GrozaVisor, groza_db, GrozaVisors, groza_visors

from tests.memory_storage import MemoryStorage
from tests.schema import TTable, TSchema, TColumn, TType, TRow


def test_fetch_sub(groza_storage):
    schema = TSchema(
        tables=[
            TTable("accounts", [
                TColumn("id", TType.INT8),
                TColumn("name", TType.STR),
            ], data=[
                TRow({"id": 1, "name": "aaa"}),
                TRow( {"id": 2, "name": "bbb"}),
            ]),
        ])

    groza_storage.setup(schema)

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


def test_insert(groza_storage):
    schema = TSchema(
        tables=[
            TTable("accounts", [
                TColumn("id", TType.INT8),
                TColumn("name", TType.STR),
            ], data=[
                TRow({"id": 1, "name": "aaa"}),
                TRow({"id": 2, "name": "bbb"}),
            ]),
        ]
    )

    groza_storage.setup(schema)

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


def test_update(groza_storage):
    schema = TSchema(
        tables=[
            TTable("accounts", [
                TColumn("id", TType.INT8),
                TColumn("name", TType.STR),
            ], data=[
                TRow({"id": 1, "name": "aaa"}),
                TRow({"id": 2, "name": "bbb"}),
            ]),
        ]
    )
    groza_storage.setup(schema)

    class Account(GrozaVisor):
        table = "accounts"
        primary_key = "id"

    groza = Groza()
    update = [
        [{"visor": "Account", "id": 1}, {"name": "aaa1"}],
    ]
    resp = asyncio.get_event_loop().run_until_complete(groza.query_update(user=GrozaUser(), update=update))
    assert resp.data["status"] == "ok"

    assert schema.tables["accounts"].data[0]["id"] == 1
    assert schema.tables["accounts"].data[0]["name"] == "aaa1"


if __name__ == "__main__":
    pytest.main(["-s", "-x", __file__])
