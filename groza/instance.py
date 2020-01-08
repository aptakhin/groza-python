import asyncio
import hashlib
from uuid import UUID

from groza import GrozaUser, GrozaRequest, GrozaResponse
from groza.auth.debug import DebugAuth

from groza.storage import GrozaStorage, groza_db, groza_visors, GrozaVisor

SECRET_KEY = ";!FC,gvn58QUHok}ZKb]23.iXE<01?MkRVz-YL>T:iU6tlS89'yWaY&b_NE?5xsM"


def hashit(passw):
    if not passw:
        raise ValueError('Empty password')
    # if len(passw) < 6:
    #     raise ValueError('Too short password')
    m = hashlib.sha3_256()
    m.update((SECRET_KEY + passw).encode())
    return m.hexdigest()


class Groza:
    def __init__(self):
        # self.tables = tables
        self._storage: GrozaStorage = groza_db.get()
        self._auth = DebugAuth()

    async def register(self, request: GrozaRequest) -> GrozaResponse:
        user = self._auth.register(request)

        return GrozaResponse({}, request=request)
        # return {'status': 'ok', 'token': token, 'userId': auth['userId'], 'type': 'register'}

    async def login(self, request: GrozaRequest) -> GrozaResponse:
        user = self._auth.login(request)
        return GrozaResponse({}, request=request)

    async def auth(self, request: GrozaRequest) -> GrozaResponse:
        # user = self.auth.register(request)
        return GrozaResponse({}, request=request)

    async def fetch_sub(self, user, all_sub):
        resp = {}
        data = {}
        errors = []

        sub_resp = {}

        async with self._storage.session() as session:
            for sub, sub_desc in all_sub.items():
                visor_name = sub_desc['visor']
                # if table not in self.tables:
                #     errors.append(f'Table '{table}' is not handled')
                #     continue
                #
                # sub_table = self.tables[table]

                visor = self._get_visor(visor_name)

                sub_desc_from = sub_desc.get('fromSub')
                add_data, link_field = await session.query(
                    visor=visor,
                    from_sub=sub_desc_from,
                    where=sub_desc.get('where'),
                    order=sub_desc.get('order'),
                    all_sub=all_sub,
                    sub_resp=sub_resp,
                )

                primary_key_field = visor.primary_key

                def make_key(key):
                    if isinstance(key, UUID):
                        key = str(key)
                    return key

                table = visor.table

                data.setdefault(table, {})
                data[table].update(add_data)

                ids = []
                recursive_field, recursive_inject = sub_desc.get('recursive', (None, None))

                assert (recursive_field is None and recursive_inject is None
                     or recursive_field is not None and recursive_inject is not None)

                if recursive_field:
                    for key in add_data.keys():
                        add_data[key].setdefault(recursive_inject, [])

                for key, item in add_data.items():
                    if recursive_field and item.get(recursive_field):
                        inject_to = data[table][item[recursive_field]]
                        inject_to.setdefault(recursive_inject, [])
                        inject_to[recursive_inject].append(item[primary_key_field])
                        continue

                    ids.append(make_key(item[primary_key_field]))

                from_sub = {}
                if sub_desc_from is not None:
                    for key, item in add_data.items():
                        lf = item[link_field]

                        from_sub.setdefault(lf, [])
                        from_sub[lf].append(item[primary_key_field])

                sub_resp[sub] = {
                    'status': 'ok',
                    'dataField': table,
                    'ids': ids,
                }

                sub_resp[sub]['fromSub'] = from_sub

        resp['type'] = 'data'
        resp['data'] = data
        resp['sub'] = sub_resp

        if errors:
            resp['errors'] = errors

        return GrozaResponse(resp)

    async def query_insert(self, user, query, insert):
        visor_name = query['visor']
        visor = self._get_visor(visor_name)
        # if table not in self.tables:
        #     return GrozaResponse({'errors': [f'Table '{table}' is not handled']})

        async with self._storage.session() as session:
            visor_instance = visor()
            result = await visor_instance.insert(insert=insert, user=user, session=session)

        if not result:
            return GrozaResponse({'status': 'error', 'message': 'No result'})

        return GrozaResponse({'status': 'ok', visor.primary_key: result[visor.primary_key]})

    async def query_update(self, user, update):
        for cnt, (query, upd) in enumerate(update):
            visor_name = query['visor']
            _ = self._get_visor(visor_name)

        async with self._storage.session() as session:
            async with session.transaction():
                for cnt, (query, upd) in enumerate(update):
                    visor_name = query['visor']
                    visor = self._get_visor(visor_name)

                    visor_instance = visor()
                    result = await visor_instance.update(update=(query, upd), user=user, session=session)

        return GrozaResponse({'status': 'ok'})

    async def query_delete(self, user, delete):
        for cnt, delete_item in enumerate(delete):
            visor_name = delete_item['visor']
            _ = self._get_visor(visor_name)

        async with self._storage.session() as session:
            async with session.transaction():
                for cnt, delete_item in enumerate(delete):
                    visor_name = delete_item['visor']
                    visor = self._get_visor(visor_name)

                    visor_instance = visor()
                    result = await visor_instance.delete(delete=delete_item, user=user, session=session)

        return GrozaResponse({'status': 'ok'})

    @classmethod
    def _get_visor(cls, name) -> GrozaVisor:
        model = groza_visors.get().require_visor(name)
        return model
