import argparse
import asyncio
import http
import json
from typing import List

import websockets

from groza.postgres import PostgresDB
from groza.instance import Groza
from groza.utils import build_logger, init_file_loggers, json_serial


class ServerProtocol(websockets.WebSocketServerProtocol):
    async def process_request(self, path, request_headers):
        if path == '/status/':
            return http.HTTPStatus.OK, [], b'OK\n'


class User:
    def __init__(self, auth_token=None, user_id=None):
        self.auth_token = auth_token
        self.user_id = user_id


class Connection:
    def __init__(self, handler, ws, queries):
        self.handler: Groza = handler
        self.user = User()
        self.ws = ws
        self.queries = queries
        self.log = build_logger("WS")
        self.auth_token = None
        self.all_sub = {}

    async def handle_request(self, request):
        resp = {
            "response": request["counter"],
        }

        if not isinstance(request.get("counter"), (int,)):
            resp.update({"status": "error", "message": "Invalid not integer counter"})
            return resp

        if request.get("type") not in ("login", "sub", "auth", "update", "insert"):
            return {"status": "error", "message": "Invalid type"}

        handle_resp = {}
        req_type = request["type"]
        if req_type == "login":
            handle_resp = await self.handler.login(self.user, login=request.get("login"))
            p = 0
        elif req_type == "auth":
            token = request["token"]
            handle_resp = await self.handler.auth(self.user, token=token)
            if handle_resp["status"] == "ok":
                self.user.auth_token = token
                self.user.user_id = handle_resp["userId"]
        elif req_type == "sub":
            if "sub" not in request or not isinstance(request["sub"], dict):
                return {"status": "error", "message": "Invalid not dict sub"}
            self.all_sub = request["sub"]
            handle_resp = await self.handler.fetch_sub(self.user, self.all_sub)
        elif req_type == "update":
            update = request["update"]
            handle_resp = await self.handler.query_update(self.user, update)
        elif req_type == "insert":
            query = request["query"]
            insert = request["insert"]
            handle_resp = await self.handler.query_insert(self.user, query, insert)
        else:
            raise RuntimeError("Unhandled type %s" % req_type)

        resp.update(handle_resp)

        return resp

    async def handle(self):
        async for message in self.ws:
            try:
                self.log.debug(f"Req : {message}")
                request = json.loads(message)

                handler_resp = await self.handle_request(request)

                await self.send(handler_resp)
            except:
                self.log.exception("Exception handling message: %s" % message)

    async def send(self, resp):
        js = json.dumps(resp, default=json_serial)
        self.log.debug("Resp: %s" % js)
        await self.ws.send(js)

    async def send_sub(self):
        resp = await self.handler.fetch_sub(self.user, self.all_sub)
        await self.send(resp)


class Server:
    def __init__(self):
        self.log = build_logger("Server")
        self.conns: List[Connection] = []
        self.queries = []
        self.db = PostgresDB()
        self.tables = {
            "boxes": ("boxId", {}),
            "tasks": ("taskId", {"boxes": ("boxId",)}),
            "users": ("userId", {})
        }

        self.handler = Groza(self.tables, self.db)
        self.notif_conn = None

        self.notifies = asyncio.Queue()

    async def start(self):
        await self.db.connect()
        self.log.info("Started DB")

        await self.handler.start()

        self.notif_conn = await self.db.pool.acquire()
        await self.notif_conn.add_listener("boxes", self.notify)
        await self.notif_conn.add_listener("tasks", self.notify)

    async def ws_handler(self, ws, _):
        conn = Connection(self.handler, ws, self.queries)
        self.conns.append(conn)

        self.log.debug("New connection (all: %d)" % len(self.conns))

        try:
            await conn.handle()
        finally:
            self.conns.remove(conn)

    def notify(self, conn, pid, channel, message):
        obj_id = int(message)
        self.notifies.put_nowait((channel, obj_id))

    async def loop(self):
        self.log.info("Started loop")
        while True:
            while not self.notifies.empty():
                channel, obj_id = await self.notifies.get()

                for conn in self.conns:
                    await conn.send_sub()

            await asyncio.sleep(1.)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=7700)

    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    init_file_loggers(filename="groza.log", names=["Server", "WS", "DB"])
    server = Server()
    asyncio.get_event_loop().run_until_complete(server.start())

    start_server = websockets.serve(server.ws_handler, args.host, args.port, create_protocol=ServerProtocol)
    log = build_logger("Server")

    # log.info("Running on %s:%d", args.host, args.port)
    asyncio.get_event_loop().run_until_complete(asyncio.gather(start_server, server.loop()))
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
