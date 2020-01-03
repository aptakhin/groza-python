from groza import GrozaRequest
from groza.auth import BaseAuth, User


class DebugAuth(BaseAuth):
    def register(self, request: GrozaRequest) -> User:
        pass

    def login(self, request: GrozaRequest) -> User:
        return User(user_id=1)
