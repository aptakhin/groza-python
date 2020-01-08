from abc import ABC, abstractmethod


class GrozaClientResponse(ABC):
    pass


class GrozaServerResponse(ABC):
    pass


class GrozaClientTransport(ABC):
    pass


class GrozaServerTransport(ABC):
    @abstractmethod
    async def install(self, server: 'GrozaServer'):
        pass
