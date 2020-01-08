import asyncio

import pytest

from groza import GrozaUser
from groza.instance import Groza
from groza.storage import GrozaVisor

from tests.schema import TTable, TSchema, TColumn, TType, TRow


def test_fetch_sub(groza_storage):
    schema = TSchema(
        tables=[
            TTable("accounts", [
                TColumn("id", TType.BIGSERIAL),
                TColumn("name", TType.STR),
                TColumn("last_updated_by", TType.INT8),
            ], data=[
                TRow({"id": 1, "name": "aaa", "last_updated_by": 1}),
                TRow({"id": 2, "name": "bbb", "last_updated_by": 1}),
            ]),
        ]
    )

    groza_storage.setup(schema)

    class Account(GrozaVisor):
        table = "accounts"
        primary_key = "id"

    groza = Groza()
    subscription = {
        "allAccounts": {"visor": "Account"},
    }
    resp = asyncio.get_event_loop().run_until_complete(groza.fetch_sub(GrozaUser(user_id=1), subscription))
    data = resp.data["data"]
    sub = resp.data["sub"]

    assert set(data["accounts"].keys()) == {1, 2}

    assert sub["allAccounts"]["status"] == "ok"
    assert set(sub["allAccounts"]["ids"]) == {1, 2}


def test_insert(groza_storage):
    schema = TSchema(
        tables=[
            TTable("accounts", [
                TColumn("id", TType.BIGSERIAL),
                TColumn("name", TType.STR),
                TColumn("last_updated_by", TType.INT8),
            ], data=[
                TRow({"id": 1, "name": "aaa", "last_updated_by": 1}),
                TRow({"id": 2, "name": "bbb", "last_updated_by": 1}),
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
        # "last_updated_by": 1,
    }
    resp = asyncio.get_event_loop().run_until_complete(groza.query_insert(user=GrozaUser(user_id=1), query=query, insert=insert))
    assert resp.data["id"] == 3


def test_update(groza_storage):
    schema = TSchema(
        tables=[
            TTable("accounts", [
                TColumn("id", TType.BIGSERIAL),
                TColumn("name", TType.STR),
                TColumn("last_updated_by", TType.INT8),
            ], data=[
                TRow({"id": 1, "name": "aaa", "last_updated_by": 1}),
                TRow({"id": 2, "name": "bbb", "last_updated_by": 1}),
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
    resp = asyncio.get_event_loop().run_until_complete(groza.query_update(user=GrozaUser(user_id=1), update=update))
    assert resp.data["status"] == "ok"

    data = groza_storage.query("accounts", order_field="id")

    assert data[0]["id"] == 1
    assert data[0]["name"] == "aaa1"


if __name__ == "__main__":
    pytest.main(["-s", "-x", __file__])
