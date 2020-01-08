import contextvars
from abc import abstractmethod, ABC
from typing import Union

from groza import GrozaUser


groza_db = contextvars.ContextVar('groza_db')


groza_visors = contextvars.ContextVar('groza_visors')


class GrozaCreator(type):
    def __new__(cls, name, bases, attrs):
        instance = super().__new__(cls, name, bases, attrs)

        models = groza_visors.get(None)
        if models is None:
            groza_visors.set(GrozaVisors())
        models: GrozaVisors = groza_visors.get()

        if bases and name not in models._visors_dict \
                and name not in ('GrozaForeignKey',):
            models._visors_dict[name] = instance

        def get_value(*_):
            return groza_db.get()

        instance.db = property(get_value)
        return instance


class GrozaAction:
    pass


class GrozaInput(dict):
    pass


class GrozaSession(ABC):
    @abstractmethod
    def transaction(self):
        pass

    @abstractmethod
    def raw_conn(self):
        pass

    @abstractmethod
    async def insert(self, *, visor: 'GrozaVisor',
                     insert: GrozaInput, user: GrozaUser):
        pass

    @abstractmethod
    async def update(self, *, visor: 'GrozaVisor', update, user: GrozaUser):
        pass

    @abstractmethod
    async def delete(self, *, visor: 'GrozaVisor', delete, user: GrozaUser):
        pass


class GrozaStorage:
    @abstractmethod
    def session(self) -> GrozaSession:
        return GrozaSession()

    @abstractmethod
    def release(self, session: GrozaSession):
        pass


class GrozaVisors:
    def __init__(self):
        # self._models = []
        self._visors_dict: dict = {}

    def require_visor(self, name) -> 'GrozaVisor':
        return self._visors_dict[name]

    def visor_values(self):
        return self._visors_dict.values()


class GrozaVisor(metaclass=GrozaCreator):
    def __init__(self):
        pass

    async def ensure_permission(self,
                                user: GrozaUser,
                                action: GrozaAction,
                                session):
        pass

    async def insert(self,
                     insert: GrozaInput,
                     user: GrozaUser,
                     session: GrozaSession):
        return await session.insert(visor=self, insert=insert, user=user)

    async def update(self, update, user: GrozaUser, session: GrozaSession):
        return await session.update(visor=self, update=update, user=user)

    async def delete(self, delete, user: GrozaUser, session: GrozaSession):
        return await session.delete(visor=self, delete=delete, user=user)


class GrozaForeignKey(GrozaVisor):
    def __init__(self, model: Union[type, str], field: str):
        self._model = model
        self._field = field
