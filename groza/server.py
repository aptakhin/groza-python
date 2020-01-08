import asyncio
import json
from collections import OrderedDict
from typing import List

from aiohttp import web

from groza import GrozaUser, GrozaRequest, GrozaResponse
from groza.instance import Groza
from groza.utils import build_logger, json_serial


class Connection:
    def __init__(self, handler, ws, queries):
        self.handler: Groza = handler
        self.user = GrozaUser(auth_token='', user_id=1)
        self.ws = ws
        self.queries = queries
        self.log = build_logger('WS')
        self.auth_token = None
        self.all_sub = {}
        self.last_sub = {}
        self.global_params = {}

    async def handle_request(self, request):
        resp = {
            'responseQueryId': request['queryId'],
        }

        if request.get('type') not in ('login', 'sub', 'auth', 'register', 'update', 'insert', 'delete'):
            return {'status': 'error', 'message': 'Invalid type: %s' % request.get('type')}

        push_request = GrozaRequest(request)

        req_type = request['type']
        if req_type == 'login':
            handle_resp = await self.handler.login(push_request)
            p = 0
        elif req_type == 'register':
            handle_resp = await self.handler.register(push_request)
            p = 0
        elif req_type == 'auth':
            token = request['token']
            handle_resp = await self.handler.auth(push_request)
            if handle_resp.data.get('status') == 'ok':
                self.user.auth_token = token
                self.user.user_id = handle_resp.data['userId']
        elif req_type == 'sub':
            if 'sub' not in request or not isinstance(request['sub'], dict):
                return GrozaResponse({'status': 'error', 'message': 'Invalid not dict sub'})
            self.all_sub = request['sub']
            handle_resp = await self.handler.fetch_sub(self.user, self.all_sub)
            self.last_sub = handle_resp.data['sub']
        elif req_type == 'update':
            update = request['update']
            handle_resp = await self.handler.query_update(self.user, update)
        elif req_type == 'insert':
            query = request['query']
            insert = request['insert']
            handle_resp = await self.handler.query_insert(self.user, query, insert)
        elif req_type == 'delete':
            delete = request['delete']
            handle_resp = await self.handler.query_delete(self.user, delete)
        else:
            raise RuntimeError('Unhandled type %s' % req_type)

        resp.update(handle_resp.data)

        return resp

    async def handle(self):
        async for message in self.ws:
            try:
                self.log.debug(f'Req : {message}')
                request = json.loads(message.data, object_pairs_hook=OrderedDict)

                handler_resp = await self.handle_request(request)

                await self.send(handler_resp)
            except:
                self.log.exception('Exception handling message: %s' % message.data)

    async def send(self, resp):
        js = json.dumps(resp, default=json_serial)
        self.log.debug('.. Resp: %s' % js)
        await self.ws.send_str(js)

    async def send_sub(self):
        resp = await self.handler.fetch_sub(self.user, self.all_sub)
        self.last_sub = resp.data['sub']
        await self.send(resp.data)

    async def notify_change(self, table, obj_id):
        for key, data in self.last_sub.items():
            data_table = data['dataField']
            # TODO: check links
            if table != data_table:
                continue

            if obj_id not in data.get('ids', []):
                continue

            await self.send_sub()

            break


class Server:
    def __init__(self, notifications):
        self._log = build_logger('Server')
        self.conns: List[Connection] = []
        self.queries = []

        self._notifications = notifications

    async def start(self):
        pass

    async def ws_handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        handler = Groza()

        conn = Connection(handler, ws, self.queries)
        self.conns.append(conn)

        self._log.debug('New connection (all: %d)' % len(self.conns))

        try:
            await conn.handle()
        finally:
            self.conns.remove(conn)

        return ws

    async def loop(self):
        self._log.info('Started loop')
        while True:
            while not self._notifications.empty():
                pid, channel, obj_id = await self._notifications.get()

                # TODO: Don't send full sub on same pids

                for conn in self.conns:
                    await conn.notify_change(channel, obj_id)

            await asyncio.sleep(0.3)
