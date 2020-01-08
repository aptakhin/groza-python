import asyncio
import json
from abc import abstractmethod, ABC
from collections import OrderedDict
from typing import List, Optional

from aiohttp import web

from groza import GrozaUser, GrozaRequest, GrozaResponse
from groza.queue import BaseQueue
from groza.state import GrozaHandler
from groza.transport import GrozaServerTransport
from groza.utils import build_logger, json_serial


class GrozaServerConnection:
    def __init__(self, handler, response):
        self.handler: GrozaHandler = handler
        self.user = GrozaUser(auth_token='', user_id=1)
        self.ws = response
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
        async for message in await self.ws.get_messages():
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
        await self.ws.send(js)

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


class GrozaServer(ABC):
    def __init__(self, name: str):
        self._name: str = name
        self._notifications: Optional[BaseQueue] = None

    @property
    def name(self):
        return self._name

    async def install(self, notifications: BaseQueue) -> 'GrozaServer':
        self._notifications = notifications
        return self

    @abstractmethod
    async def notify_connection_start(self, conn: GrozaServerConnection):
        pass

    @abstractmethod
    async def notify_connection_close(self, conn: GrozaServerConnection):
        pass

    @abstractmethod
    async def notify_change(self, pid, channel, obj_id):
        pass


class SimpleGrozaServer(GrozaServer):
    def __init__(self, name, transport: GrozaServerTransport):
        self._name = name
        self._log = build_logger('Server')
        self._conns: List[GrozaServerConnection] = []

        self._notifications: Optional[BaseQueue] = None

        self._transport: GrozaServerTransport = transport

    async def install(self, notifications: BaseQueue) -> 'SimpleGrozaServer':
        self._notifications = notifications
        await self._transport.install(self)
        return self

    async def notify_connection_start(self, conn: GrozaServerConnection):
        self._conns.append(conn)

    async def notify_connection_close(self, conn: GrozaServerConnection):
        self._conns.remove(conn)

    async def notify_change(self, pid, channel, obj_id):
        for conn in self._conns:
            await conn.notify_change(channel, obj_id)

