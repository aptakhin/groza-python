import asyncio
import hashlib
from uuid import UUID

from groza import User, GrozaRequest, GrozaResponse
from groza.auth.debug import DebugAuth

from groza.storage import BaseStorage, groza_db, groza_models, GrozaModel
from groza.utils import FieldTransformer, CamelCaseFieldTransformer

SECRET_KEY = ";!FC,gvn58QUHok}ZKb]23.iXE<01?MkRVz-YL>T:iU6tlS89'yWaY&b_NE?5xsM"


def hashit(passw):
    if not passw:
        raise ValueError("Empty password")
    # if len(passw) < 6:
    #     raise ValueError("Too short password")
    m = hashlib.sha3_256()
    m.update((SECRET_KEY + passw).encode())
    return m.hexdigest()


class Groza:
    def __init__(self):
        # self.tables = tables
        self._storage: BaseStorage = groza_db.get()
        self._auth = DebugAuth()
        self._field_transformer: FieldTransformer = CamelCaseFieldTransformer()

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

        async with self._storage.session() as session:
            for sub, sub_desc in all_sub.items():
                model_name = sub_desc["model"]
                # if table not in self.tables:
                #     errors.append(f"Table '{table}' is not handled")
                #     continue
                #
                # sub_table = self.tables[table]

                model = self._get_model(model_name)

                sub_desc_from = sub_desc.get("fromSub")
                items, link_field = await session.query(
                    visor=model,
                    from_sub=sub_desc_from,
                    where=sub_desc.get("where"),
                    order=sub_desc.get("order"),
                    all_sub=all_sub,
                    sub_resp=sub_resp,
                )

                primary_key_field = model.primary_key

                def make_key(key):
                    if isinstance(key, UUID):
                        key = str(key)
                    return key

                def make_item(item: dict):
                    return {self._from_db(k): v for k, v in item.items()}

                add_data = {make_key(item[primary_key_field]): make_item(item) for item in items}

                table = model.table

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
        model_name = query["model"]
        model = self._get_model(model_name)
        # if table not in self.tables:
        #     return GrozaResponse({"errors": [f"Table '{table}' is not handled"]})

        async with self._storage.session() as session:
            result = await session.insert(
                visor=model,
                insert=insert,
                user=user
            )

        if not result:
            return GrozaResponse({"status": "error", "message": "No result"})

        return GrozaResponse({"status": "ok", model.primary_key: result[model.primary_key]})

    async def query_update(self, user, update):
        for cnt, (query, upd) in enumerate(update):
            model_name = query["model"]
            _ = self._get_model(model_name)

        async with self._storage.session() as session:
            async with session.transaction():
                for cnt, (query, upd) in enumerate(update):
                    model_name = query["model"]
                    visor = self._get_model(model_name)

                    await session.update(
                        visor=visor,
                        update=update,
                        user=user
                    )

        return GrozaResponse({"status": "ok"})

    @classmethod
    def _get_model(cls, name) -> GrozaModel:
        model = groza_models.get().require_model(name)
        return model

    def _from_db(self, field):
        return self._field_transformer.from_db(field)

    def _to_db(self, field):
        return self._field_transformer.to_db(field)


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
        res = res.data

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
