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
        self.last_sub = resp["sub"]
        await self.send(resp)

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

        # await self.handler.start()

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
        obj_id = int(message)
        self.notifies.put_nowait((channel, obj_id))

    async def loop(self):
        self.log.info("Started loop")
        while True:
            while not self.notifies.empty():
                channel, obj_id = await self.notifies.get()

                for conn in self.conns:
                    await conn.notify_change(channel, obj_id)

            await asyncio.sleep(0.1)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=7700)

    args = parser.parse_args()
    return args


# def main():
#     args = parse_args()
#
#     init_file_loggers(filename="groza.log", names=["Server", "WS", "DB"])
#     server = Server()
#     asyncio.get_event_loop().run_until_complete(server.start())
#
#     start_server = websockets.serve(server.ws_handler, args.host, args.port, create_protocol=ServerProtocol)
#     log = build_logger("Server")
#
#     # log.info("Running on %s:%d", args.host, args.port)
#     asyncio.get_event_loop().run_until_complete(asyncio.gather(start_server, server.loop()))
#     asyncio.get_event_loop().run_forever()
#
#
# if __name__ == "__main__":
#     main()
