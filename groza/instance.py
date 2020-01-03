import asyncio
import hashlib
import json
from datetime import datetime, timedelta
from uuid import UUID

from groza import User, GrozaRequest, GrozaResponse
from groza.auth import BaseAuth
from groza.auth.debug import DebugAuth
from groza.postgres import PostgresDB
from groza.q import Q

SECRET_KEY = ";!FC,gvn58QUHok}ZKb]23.iXE<01?MkRVz-YL>T:iU6tlS89'yWaY&b_NE?5xsM"


def hashit(passw):
    if not passw:
        raise ValueError("Empty password")
    # if len(passw) < 6:
    #     raise ValueError("Too short password")
    m = hashlib.sha3_256()
    m.update((SECRET_KEY + passw).encode())
    return m.hexdigest()


class Table:
    def __init__(self, table, primary_key, foreign=None):
        self.table = table
        self.primary_key = primary_key

        self.foreign = foreign if foreign is not None else {}


class Groza:
    def __init__(self, tables, connectors):
        self.tables = tables
        self.connectors = connectors
        self.main_db = connectors.request()
        self.data_dbname = None
        self.data_db: PostgresDB = self.main_db
        self._auth = DebugAuth()

    # async def start(self):
    #     await self.start_tables()

    # @property
    # def auth(self) -> BaseAuth:
    #     return self._auth

    async def setup_data_db(self, dbname):
        self.data_dbname = dbname
        # company = await self.main_db.fetchrow('SELECT "companyId" FROM companies WHERE slug=$1', dbname)
        # name = "c_%d" % company["companyId"]
        self.data_db = self.connectors.request(None)
        # await self.data_db.connect()
        # await self.start_tables()

    async def register(self, request: GrozaRequest) -> GrozaResponse:
        user = self._auth.register(request)

        return GrozaResponse({}, request=request)
        # return {"status": "ok", "token": token, "userId": auth["userId"], "type": "register"}

    async def login(self, request: GrozaRequest) -> GrozaResponse:
        user = self._auth.login(request)
        return GrozaResponse({}, request=request)

    async def auth(self, request: GrozaRequest) -> GrozaResponse:
        # user = self.auth.register(request)
        return GrozaResponse({}, request=request)

    async def fetch_sub(self, user, all_sub):
        resp = {}
        data = {}
        errors = []

        sub_resp = {}

        for sub, sub_desc in all_sub.items():
            table = sub_desc["table"]
            if table not in self.tables:
                errors.append(f"Table '{table}' is not handled")
                continue

            sub_table = self.tables[table]

            q = Q.SELECT().FROM(table)

            primary_key_field = sub_table[0]

            link_field = None
            sub_desc_from = sub_desc.get("fromSub")
            if sub_desc_from is not None:
                if sub_desc_from not in sub_resp:
                    errors.append(f"Link '{sub_desc_from}' not found in results. Check identifiers and order")
                    continue

                link_table = all_sub[sub_desc_from]["table"]

                link_field = sub_table[1].get(link_table)
                if not link_field:
                    errors.append(f"Link '{table}'=>'{link_table}' not found")
                    continue

                link_field = link_field[0]

                q.WHERE(link_field, Q.Any(sub_resp[sub_desc_from]["ids"]))

            if sub_desc.get("where"):
                for field, value in sub_desc["where"].items():
                    q.WHERE(field, value)

            if sub_desc.get("order"):
                for field, order in sub_desc["order"].items():
                    q.ORDER(field, order)

            db = self.data_db
            if table == "users":
                db = self.main_db
            items = list(await db.fetch(q))

            def make_key(key):
                if isinstance(key, UUID):
                    key = str(key)
                return key

            add_data = {make_key(item[primary_key_field]): dict(item) for item in items}

            # if table == "tasks":
            #     for i in range(200, 1200):
            #         add_data[i] = dict(add_data[117])
            #         add_data[i]["taskId"] = i

            data.setdefault(table, {})
            data[table].update(add_data)

            ids = []
            recursive_field, recursive_inject = sub_desc.get("recursive", (None, None))

            assert (recursive_field is None and recursive_inject is None
                 or recursive_field is not None and recursive_inject is not None)

            if recursive_field:
                for key in add_data.keys():
                    add_data[key].setdefault(recursive_inject, [])

            for key, item in add_data.items():
                if recursive_field and item.get(recursive_field):
                    inject_to = data[table][item[recursive_field]]
                    inject_to.setdefault(recursive_inject, [])
                    inject_to[recursive_inject].append(item[primary_key_field])
                    continue

                ids.append(make_key(item[primary_key_field]))

            from_sub = {}
            if sub_desc_from is not None:
                for key, item in add_data.items():
                    lf = item[link_field]

                    from_sub.setdefault(lf, [])
                    from_sub[lf].append(item[primary_key_field])

            sub_resp[sub] = {
                "status": "ok",
                "dataField": table,
                "ids": ids,
            }

            sub_resp[sub]["fromSub"] = from_sub

        resp["type"] = "data"
        resp["data"] = data
        resp["sub"] = sub_resp

        if errors:
            resp["errors"] = errors

        return GrozaResponse(resp)

    async def query_insert(self, user, query, insert):
        resp = {}
        table = query["table"]
        if table not in self.tables:
            return {"errors": [f"Table '{table}' is not handled"]}

        desc = self.tables[table]

        idx = 1
        args = ()
        own_fields = []
        own_values = []

        for key, value in insert.items():
            if key == desc[0]:
                continue

            own_fields.append(f'"{key}"')
            own_values.append(f"${idx}")
            args += (value,)
            idx += 1

        # own_fields.append('"lastUpdatedBy"')
        # own_values.append(f"${idx}")
        # args += (user.user_id,)
        # idx += 1

        own_fields_str = ", ".join(own_fields)
        own_values_str = ", ".join(own_values)

        primary_key = desc[0]

        query = f'INSERT INTO {table} ({own_fields_str}) VALUES ({own_values_str}) RETURNING "{primary_key}"'
        result = await self.data_db.fetchrow(query, *args)
        if not result:
            return {"status": "error", "message": "No result"}

        return GrozaResponse({"status": "ok", primary_key: result[primary_key]})

    async def query_update(self, user, update):
        for cnt, (query, upd) in enumerate(update):
            table = query["table"]
            if table not in self.tables:
                return {"errors": [f"Table '{table}' in #{cnt} row is not handled"]}

        async with self.data_db.pool.acquire() as conn:
            async with conn.transaction():
                for cnt, (query, upd) in enumerate(update):
                    table = query["table"]
                    desc = self.tables[table]

                    idx = 1
                    args = ()
                    fields = []
                    for key, value in upd.items():
                        fields.append(f'"{key}" = ${idx}')
                        args += (value,)
                        idx += 1

                    # fields.append(f'"lastUpdatedBy" = ${idx}')
                    # args += (user.user_id,)
                    # idx += 1

                    value_fields = ", ".join(fields)

                    primary_key_field = desc[0]

                    args += (query[primary_key_field],)
                    primary_key_idx = idx

                    idx += 1
                    query_str = f'UPDATE {table} SET {value_fields} WHERE "{primary_key_field}" = ${primary_key_idx}'
                    await conn.execute(query_str, *args)

        return GrozaResponse({"status": "ok"})


def test():
    async def t():
        db = PostgresDB("ridger_dev")
        await db.connect()

        boxes = "test_groza_boxes"
        tasks = "test_groza_tasks"

        async with db.pool.acquire() as conn:
            await conn.execute(f'DROP TABLE IF EXISTS {boxes}')
            await conn.execute(f'DROP TABLE IF EXISTS {tasks}')

            await conn.execute(f"""
                CREATE TABLE {boxes} (
                    "boxId" serial PRIMARY KEY,
                    "title" varchar
                )
            """)

            await conn.execute(f"""
                CREATE TABLE {tasks} (
                    "taskId" serial PRIMARY KEY,
                    "boxId" int4 NOT NULL,
                    "title" varchar,
                    "parentBoxId" int4
                )
            """)

            await conn.execute(f'INSERT INTO {boxes} ("boxId", "title") VALUES ($1, $2)', 1, "First Box")

            await conn.execute(f'INSERT INTO {tasks} ("taskId", "boxId", "title") VALUES ($1, $2, $3) RETURNING "taskId"', 1, 1, "First Task")
            await conn.execute(f'INSERT INTO {tasks} ("taskId", "boxId", "title", "parentBoxId") VALUES ($1, $2, $3, $4)', 2, 1, "First Sub Task", 1)
            await conn.execute(f'INSERT INTO {tasks} ("taskId", "boxId", "title") VALUES ($1, $2, $3)', 3, 1, "Second Task")

            await conn.execute(f'INSERT INTO {boxes} ("boxId", "title") VALUES ($1, $2)', 2, "Second Box")
            await conn.execute(f'INSERT INTO {tasks} ("taskId", "boxId", "title") VALUES ($1, $2, $3)', 4, 2, "First Task of Second Box")


        tables = {
            boxes: ("boxId", {}),
            tasks: ("taskId", {boxes: ("boxId",)}),
        }

        # tables2 = {
        #     boxes: Table("boxId", foreign={"taskIds": Foreign(tasks, "boxId")}),
        #     tasks: Table("taskId", foreign={boxes: "boxId"}),
        # }

        groza = Groza(tables, db)

        subscription = {
            "allBoxes": {"table": boxes},
            "boxTasks": {"table": tasks, "fromSub": "allBoxes", "recursive": ("parentBoxId", "childrenIds")},
        }

        res = await groza.fetch_sub(User(), subscription)

        assert set(res["sub"]["allBoxes"]["ids"]) == {1, 2}
        assert set(res["sub"]["boxTasks"]["ids"]) == {1, 3, 4}
        assert res["sub"]["boxTasks"]["fromSub"] == {1: [1, 2, 3], 2: [4]}

        assert res["data"][tasks][1]["childrenIds"] == [2]

        assert res["data"][tasks][2]["childrenIds"] == []
        assert res["data"][tasks][3]["childrenIds"] == []
        assert res["data"][tasks][4]["childrenIds"] == []

        assert all((isinstance(task_data["childrenIds"], list)
                    for task_data in res["data"][tasks].values()))

        for sub, sub_resp in res["sub"].items():
            sent_id_not_in_data = set(sub_resp["ids"]) - set(res["data"][subscription[sub]["table"]])
            assert not bool(sent_id_not_in_data)

    asyncio.get_event_loop().run_until_complete(t())


if __name__ == "__main__":
    test()
