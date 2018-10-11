import asyncio
import hashlib
import json
from datetime import datetime, timedelta

from groza.postgres import PostgresDB
from groza.q import Q, QSafe

SECRET_KEY = ";!FC,gvn58QUHok}ZKb]23.iXE<01?MkRVz-YL>T:iU6tlS89'yWaY&b_NE?5xsM"


def hashit(passw):
    if not passw:
        raise ValueError("Empty password")
    # if len(passw) < 6:
    #     raise ValueError("Too short password")
    m = hashlib.sha3_256()
    m.update((SECRET_KEY + passw).encode())
    return m.hexdigest()


def parse_subscription(sub: str):
    ind = sub.rfind("_")
    if ind == -1:
        table = sub
        key = None
    else:
        table = sub[:ind]
        key = int(sub[ind + 1:])
    return table, key


class Table:
    def __init__(self, table, primary_key, foreign=None):
        self.table = table
        self.primary_key = primary_key

        self.foreign = foreign if foreign is not None else {}


class Groza:
    def __init__(self, tables, db: PostgresDB):
        self.tables = tables
        self.db = db

    async def start(self):
        await self.start_tables()

    async def login(self, user, login):
        method = login["method"]
        if method == "password":
            email = login.get("data", {}).get("email")
            if not email:
                raise ValueError("Password can't be empty")
            password = login.get("data", {}).get("password")
            if not password:
                raise ValueError("Password can't be empty")

            passhash = hashit(password)
            auth = await self.db.fetchrow("""
                SELECT * FROM users_logins WHERE type=$1 AND main=$2 AND secondary=$3
            """, method, email, passhash)
            if not auth:
                return {"status": "error", "code": 2, "message": "Can't find email/password pair"}

            valid_until = datetime.now() + timedelta(days=1)
            token = "abdasdas"

            device = login.get("device", {})
            add_data = {
                "device": device,
            }
            q = Q.INSERT("users_auths").SET(
                lastUpdatedBy=auth["userId"],
                userLoginId=auth["userLoginId"],
                userId=auth["userId"],
                timeCreated=QSafe("now()"),
                timeLastAccess=QSafe("now()"),
                validUntil=valid_until,
                token=token,
                data=json.dumps(add_data),
            )
            # await self.db.execute(q)

            return {"status": "ok", "token": token, "userId": auth["userId"], "type": "login"}

        return {"status": "error", "code": 1}

    async def auth(self, user, token):
        auth = await self.db.fetchrow("""
            SELECT "userId", "validUntil" FROM users_auths WHERE token=$1
        """, token)

        if auth["validUntil"] < datetime.now():
            return {"status": "error", "message": "Token expired", "code": 3}

        await self.db.execute("""
            UPDATE users_auths SET "timeLastAccess"=now() WHERE token=$1
        """, token)

        return {"status": "ok", "type": "auth", "userId": auth["userId"]}


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

            query = f"SELECT * FROM {table}"
            args = ()
            idx = 1

            primary_key_field = sub_table[0]

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

                query += f' WHERE "{link_field}" = ANY(${idx})'
                args += (sub_resp[sub_desc_from]["ids"],)
                idx += 1

            items = list(await self.db.fetch(query, *args))

            add_data = {item[primary_key_field]: dict(item) for item in items}

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

                ids.append(item[primary_key_field])

            if sub_desc.get("inject"):
                inject_to = {}
                for iid in ids:
                    item = add_data[iid]
                    inject_to.setdefault(item[link_field], [])
                    inject_to[item[link_field]].append(item[primary_key_field])

                for key in data[link_table].keys():
                    assert sub_desc["inject"] not in data[link_table][key]
                    data[link_table][key][sub_desc["inject"]] = []

                for inj, values in inject_to.items():
                    data[link_table][inj][sub_desc["inject"]] = values
                    data[link_table][inj][sub_desc["inject"] + "Table"] = table

            sub_resp[sub] = {
                "status": "ok",
                "dataField": table,
                "ids": ids,
            }

        resp["type"] = "data"
        resp["data"] = data
        resp["sub"] = sub_resp

        if errors:
            resp["errors"] = errors

        return resp

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

        own_fields.append('"lastUpdatedBy"')
        own_values.append(f"${idx}")
        args += (user.user_id,)
        idx += 1

        own_fields_str = ", ".join(own_fields)
        own_values_str = ", ".join(own_values)

        primary_key = desc[0]

        query = f'INSERT INTO {table} ({own_fields_str}) VALUES ({own_values_str}) RETURNING "{primary_key}"'
        result = await self.db.fetchrow(query, *args)
        if not result:
            return {"status": "error", "message": "No result"}

        return {"status": "ok", primary_key: result[primary_key]}

    async def query_update(self, user, update):
        for cnt, (query, upd) in enumerate(update):
            table = query["table"]
            if table not in self.tables:
                return {"errors": [f"Table '{table}' in #{cnt} row is not handled"]}

        async with self.db.pool.acquire() as conn:
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

                    fields.append(f'"lastUpdatedBy" = ${idx}')
                    args += (user.user_id,)
                    idx += 1

                    value_fields = ", ".join(fields)

                    primary_key_field = desc[0]

                    args += (query[primary_key_field],)
                    primary_key_idx = idx

                    idx += 1
                    query_str = f'UPDATE {table} SET {value_fields} WHERE "{primary_key_field}" = ${primary_key_idx}'
                    await conn.execute(query_str, *args)

        return {"status": "ok"}

    async def start_tables(self):
        audit_table = "groza_audit"
        audit_table_seq = f"{audit_table}_id_seq"
        changed_by_field = f"updatedBy"

        await self.db.execute(f'CREATE SEQUENCE IF NOT EXISTS "{audit_table_seq}"')

        await self.db.execute(f"""
            CREATE TABLE IF NOT EXISTS "{audit_table}" (
                "audit_id" int8 PRIMARY KEY,
                "{changed_by_field}" int8 NOT NULL,
                "int_key" int8,
                "time" timestamp NOT NULL,
                "operation" bpchar NOT NULL,
                "table" varchar NOT NULL,
                "var_key" varchar,
                "o" hstore,
                "n" hstore
            );
        """)

        for table, table_desc in self.tables.items():
            lower_table = table.lower()
            audit_prefix_table = f"{lower_table}_audit"
            audit_table_func = f"{audit_prefix_table}_func"
            audit_table_trigger = f"{audit_prefix_table}_trigger"

            last_updated_by_field = f"lastUpdatedBy"

            primary_key_field = table_desc[0]

            use_int_key = f'OLD."{primary_key_field}"' if True else 'NULL'
            use_var_key = f'OLD."{primary_key_field}"' if False else 'NULL'

            use_int_key_new = f'NEW."{primary_key_field}"' if True else 'NULL'
            use_var_key_new = f'NEW."{primary_key_field}"' if False else 'NULL'

            await self.db.execute(f"""
                CREATE OR REPLACE FUNCTION "{audit_table_func}"() RETURNS TRIGGER AS 
                $$ 
                DECLARE 
                  r record;
                  oldh hstore;
                  o hstore := hstore('');
                  n hstore := hstore('');
                BEGIN 
                  IF (TG_OP = 'DELETE') THEN
                    INSERT INTO "{audit_table}" SELECT nextval('{audit_table_seq}'), OLD."{last_updated_by_field}", {use_int_key}, now(), 'D', '{table}', {use_var_key}, hstore(OLD), hstore('');
                    PERFORM pg_notify('{table}', OLD."{primary_key_field}"::text);
                    RETURN OLD;
                  ELSIF (TG_OP = 'UPDATE') THEN
                    oldh = hstore(OLD);
                    FOR r IN SELECT * FROM EACH(hstore(NEW)) 
                    LOOP 
                      IF (oldh->r.key != r.value) THEN 
                        o = o || ('"' || r.key || '" => "' || (oldh->r.key) || '"')::hstore;
                        n = n || ('"' || r.key || '" => "' || r.value || '"')::hstore;
                      END IF;
                    END LOOP; 
                    INSERT INTO "{audit_table}" SELECT nextval('{audit_table_seq}'), NEW."{last_updated_by_field}", {use_int_key}, now(), 'U', '{table}', {use_var_key}, o, n;
                    PERFORM pg_notify('{table}', NEW."{primary_key_field}"::text);
                    RETURN NEW;
                  ELSIF (TG_OP = 'INSERT') THEN
                     INSERT INTO "{audit_table}" SELECT nextval('{audit_table_seq}'), NEW."{last_updated_by_field}", {use_int_key_new}, now(), 'I', '{table}', {use_var_key_new}, hstore(''), hstore(NEW);
                     PERFORM pg_notify('{table}', NEW."{primary_key_field}"::text);
                     RETURN NEW;
                  END IF;
                  RETURN NULL;
                END; 
                $$ language plpgsql; 
            """)

            await self.db.execute(f"""
                DROP TRIGGER IF EXISTS "{audit_table_trigger}" ON "{table}"
            """)

            await self.db.execute(f"""
                CREATE TRIGGER "{audit_table_trigger}"
                    AFTER INSERT OR UPDATE OR DELETE ON "{table}"
                    FOR EACH ROW EXECUTE PROCEDURE "{audit_table_func}"()
            """)


def test():
    async def t():
        db = PostgresDB()
        await db.connect()

        boxes = "testGrozaBoxes"
        tasks = "testGrozaTasks"

        async with db.pool.acquire() as conn:

            await conn.execute(f"DROP TABLE IF EXISTS {boxes}")
            await conn.execute(f"DROP TABLE IF EXISTS {tasks}")

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
            first_task = await conn.fetchrow(f'INSERT INTO {tasks} ("boxId", "title") VALUES ($1, $2) RETURNING "taskId"', 1, "First Task")
            await conn.execute(f'INSERT INTO {tasks} ("boxId", "title", "parentBoxId") VALUES ($1, $2, $3)', 1, "First Sub Task", first_task["taskId"])
            await conn.execute(f'INSERT INTO {tasks} ("boxId", "title") VALUES ($1, $2)', 1, "Second Task")

            await conn.execute(f'INSERT INTO {boxes} ("boxId", "title") VALUES ($1, $2)', 2, "Second Box")
            await conn.execute(f'INSERT INTO {tasks} ("boxId", "title") VALUES ($1, $2)', 2, "First Task of Second Box")

        tables = {
            boxes: Table("boxId", {}),
            tasks: Table("taskId", {boxes: ("boxId",)}),
        }

        # tables2 = {
        #     boxes: Table("boxId", foreign={"taskIds": Foreign(tasks, "boxId")}),
        #     tasks: Table("taskId", foreign={boxes: "boxId"}),
        # }

        groza = Groza(tables, db)

        subscription = {
            "allBoxes": {"table": boxes},
            "boxTasks": {"table": tasks, "fromSub": "allBoxes", "inject": "taskIds", "recursive": ("parentBoxId", "childrenIds")},
        }

        res = await groza.fetch_sub(subscription)

        assert set(res["sub"]["allBoxes"]["ids"]) == {1, 2}
        assert set(res["sub"]["boxTasks"]["ids"]) == {1, 3, 4}

        assert set(res["data"][boxes][1]["taskIds"]) == {1, 3}
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