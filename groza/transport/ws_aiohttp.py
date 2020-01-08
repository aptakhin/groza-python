from aiohttp.web_ws import WebSocketResponse

from groza.server import GrozaServerConnection, GrozaServer
from groza.state import GrozaHandler
from groza.transport import GrozaClientTransport, GrozaServerTransport, \
    GrozaServerResponse


class WsAiohttpClientTransport(GrozaClientTransport):
    pass


class WsAiohttpServerTransportResponse(GrozaServerResponse):
    def __init__(self, ws: WebSocketResponse):
        self.ws: WebSocketResponse = ws

    async def get_messages(self):
        return self.ws

    async def send(self, js):
        await self.ws.send_str(js)


class WsAiohttpServerTransport(GrozaServerTransport):
    def __init__(self, server=None):
        self._server: GrozaServer = server

    async def install(self, server: GrozaServer):
        self._server = server

    async def handle(self, request):
        ws = WebSocketResponse()
        await ws.prepare(request)

        handler = GrozaHandler()

        conn = GrozaServerConnection(handler,
                                     WsAiohttpServerTransportResponse(ws))

        await self._server.notify_connection_start(conn)

        try:
            await conn.handle()
        finally:
            await self._server.notify_connection_close(conn)

        return ws
