import argparse
import asyncio
import http
import json
from collections import OrderedDict
from typing import List

import websockets
from aiohttp import web

from groza import User, GrozaRequest
from groza.postgres import PostgresDB
from groza.instance import Groza
from groza.utils import build_logger, init_file_loggers, json_serial


class ServerProtocol(websockets.WebSocketServerProtocol):
    async def process_request(self, path, request_headers):
        if path == '/status/':
            return http.HTTPStatus.OK, [], b'OK\n'


class Connection:
    def __init__(self, handler, ws, queries):
        self.handler: Groza = handler
        self.user = User()
        self.ws = ws
        self.queries = queries
        self.log = build_logger("WS")
        self.auth_token = None
        self.all_sub = {}
        self.last_sub = {}
        self.global_params = {}

    # self.handler.setup_data_db(dbname)

    async def handle_request(self, request):
        resp = {
            "response": request["counter"],
        }

        if not isinstance(request.get("counter"), (int,)):
            resp.update({"status": "error", "message": "Invalid not integer counter"})
            return resp

        if request.get("type") not in ("login", "sub", "auth", "register", "update", "insert"):
            return {"status": "error", "message": "Invalid type"}

        push_request = GrozaRequest(request)

        req_type = request["type"]
        if req_type == "login":
            handle_resp = await self.handler.login(push_request)
            p = 0
        elif req_type == "register":
            handle_resp = await self.handler.register(push_request)
            p = 0
        elif req_type == "auth":
            token = request["token"]
            handle_resp = await self.handler.auth(push_request)
            if handle_resp.data.get("status") == "ok":
                self.user.auth_token = token
                self.user.user_id = handle_resp.data["userId"]
        elif req_type == "sub":
            if "sub" not in request or not isinstance(request["sub"], dict):
                return {"status": "error", "message": "Invalid not dict sub"}
            self.all_sub = request["sub"]
            # self.global_params = request["global"]

            # await self.handler.setup_data_db(self.global_params["company"])

            handle_resp = await self.handler.fetch_sub(self.user, self.all_sub)
            self.last_sub = handle_resp.data["sub"]
        elif req_type == "update":
            update = request["update"]
            handle_resp = await self.handler.query_update(self.user, update)
        elif req_type == "insert":
            query = request["query"]
            insert = request["insert"]
            handle_resp = await self.handler.query_insert(self.user, query, insert)
        else:
            raise RuntimeError("Unhandled type %s" % req_type)

        resp.update(handle_resp.data)

        return resp

    async def handle(self):
        async for message in self.ws:
            try:
                self.log.debug(f"Req : {message}")
                request = json.loads(message.data, object_pairs_hook=OrderedDict)

                handler_resp = await self.handle_request(request)

                await self.send(handler_resp)
            except:
                self.log.exception("Exception handling message: %s" % message.data)

    async def send(self, resp):
        js = json.dumps(resp, default=json_serial)
        self.log.debug("Resp: %s" % js)
        await self.ws.send_str(js)

    async def send_sub(self):
        resp = await self.handler.fetch_sub(self.user, self.all_sub)
        self.last_sub = resp.data["sub"]
        await self.send(resp.data)

    async def notify_change(self, table, obj_id):
        for key, data in self.last_sub.items():
            data_table = data["dataField"]
            # TODO: проверять связность
            if table != data_table:
                continue

            if obj_id not in data.get("ids", []):
                continue

            await self.send_sub()

            break


class Connectors:
    def __init__(self, db_dsn):
        self.db_dsn = db_dsn
        self.dbs = {
            None: PostgresDB(self.db_dsn),
        }

    def request(self, connector_field=None):
        dbs = self.dbs.get(connector_field)
        if not dbs:
            dbs = PostgresDB(connector_field)
            self.dbs[connector_field] = dbs
        return dbs


class Server:
    def __init__(self, tables, db_dsn):
        self.log = build_logger("Server")
        self.conns: List[Connection] = []
        self.queries = []
        self.db_dsn = db_dsn
        self.connectors = Connectors(db_dsn)
        self.db = self.connectors.request(None)
        self.tables = tables

        self.notif_conn = None

        self.notifies = asyncio.Queue()

    async def start(self):
        await self.db.connect()
        self.log.info("Started DB")

        self.notif_conn = await self.db.pool.acquire()

        for table in self.tables:
            await self.notif_conn.add_listener(table, self.notify)

    async def ws_handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        handler = Groza(self.tables, self.connectors)

        conn = Connection(handler, ws, self.queries)
        self.conns.append(conn)

        self.log.debug("New connection (all: %d)" % len(self.conns))

        try:
            await conn.handle()
        finally:
            self.conns.remove(conn)

        return ws

    def notify(self, conn, pid, channel, message):
        obj_id = message
        self.notifies.put_nowait((channel, obj_id))

    async def loop(self):
        self.log.info("Started loop")
        while True:
            while not self.notifies.empty():
                channel, obj_id = await self.notifies.get()

                for conn in self.conns:
                    await conn.notify_change(channel, obj_id)

            await asyncio.sleep(0.3)

    async def add_loop(self, app):
        app['db_notif_listener'] = asyncio.create_task(self.loop())

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

            use_int_key = f'OLD."{primary_key_field}"' if False else 'NULL'
            use_var_key = f'OLD."{primary_key_field}"' if True else 'NULL'

            use_int_key_new = f'NEW."{primary_key_field}"' if False else 'NULL'
            use_var_key_new = f'NEW."{primary_key_field}"' if True else 'NULL'

            data_table = f"{table}"

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
                    PERFORM pg_notify('{data_table}', OLD."{primary_key_field}"::text);
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
                    PERFORM pg_notify('{data_table}', NEW."{primary_key_field}"::text);
                    RETURN NEW;
                  ELSIF (TG_OP = 'INSERT') THEN
                    INSERT INTO "{audit_table}" SELECT nextval('{audit_table_seq}'), NEW."{last_updated_by_field}", {use_int_key_new}, now(), 'A', '{table}', {use_var_key_new}, hstore(''), hstore(NEW);
                    PERFORM pg_notify('{data_table}', NEW."{primary_key_field}"::text);
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
