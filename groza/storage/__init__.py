import contextvars
from abc import abstractmethod
from typing import Union

from groza import User


groza_db = contextvars.ContextVar("groza_db")

groza_models = contextvars.ContextVar("groza_models")


class GrozaCreator(type):
    def __new__(cls, name, bases, attrs):
        instance = super().__new__(cls, name, bases, attrs)

        models = groza_models.get(None)
        if models is None:
            groza_models.set(GrozaModels())
        models: GrozaModels = groza_models.get()

        if bases and name not in models._models_dict and name not in ("GrozaForeignKey",):
            models._models_dict[name] = instance
            # models._models_list.append(instance)

        def get_value(*_):
            return groza_db.get()

        instance.db = property(get_value)
        return instance


class GrozaAction:
    pass


class GrozaModels:
    def __init__(self):
        # self._models = []
        self._models_dict: dict = {}

    def require_model(self, name) -> "GrozaModel":
        return self._models_dict[name]


class GrozaModel(metaclass=GrozaCreator):
    def __init__(self):
        self.table: str = ""
        self.primary_key: str = ""

    @abstractmethod
    async def ensure_permission(self, user: User, action: GrozaAction, session):
        pass

    @abstractmethod
    async def insert(self, data: dict):
        pass

    @abstractmethod
    async def update(self, data: dict):
        pass


class GrozaForeignKey(GrozaModel):
    def __init__(self, model: Union[type, str], field: str):
        self._model = model
        self._field = field

    # async def ensure_permission(self, user: User, action: GrozaAction, trans):
    #     pass
    #
    # async def insert(self, data: dict):
    #     pass
    #
    # async def update(self, data: dict):
    #     pass


class BaseSession:
    @abstractmethod
    def insert(self, model):
        pass

    @abstractmethod
    def update(self, model):
        pass


class BaseStorage:
    @abstractmethod
    def session(self) -> BaseSession:
        return BaseSession()

    @abstractmethod
    def release(self, session: BaseSession):
        pass

