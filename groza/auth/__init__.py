from abc import abstractmethod

from groza import User, GrozaRequest


class BaseAuth:
    @abstractmethod
    def register(self, request: GrozaRequest) -> User:
        pass

    @abstractmethod
    def login(self, request: GrozaRequest) -> User:
        pass
