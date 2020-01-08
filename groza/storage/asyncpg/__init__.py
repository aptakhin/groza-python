from typing import Iterable
from uuid import UUID

from groza import GrozaUser
from groza.storage.asyncpg.impl import _PostgresBackend, _PostgresConn
from groza.utils import build_logger, FieldTransformer, \
    CamelCaseFieldTransformer

from groza.storage import GrozaStorage, GrozaSession, groza_visors, GrozaVisor
from pssq import Q


class AsyncpgSession(GrozaSession):
    def __init__(self, *, conn: '_PostgresPoolProxy', log):
        self._conn = conn
        self._log = log
        self._field_transformer: FieldTransformer = CamelCaseFieldTransformer()

    async def query(self, *, visor, from_sub, all_sub, sub_resp,
                    where=None, order=None):
        q = Q.select().from_(visor.table)

        # primary_key_field = sub_table[0]

        link_field = None
        sub_desc_from = from_sub
        if sub_desc_from is not None:
            if sub_desc_from not in sub_resp:
                raise RuntimeError(f'Link "{sub_desc_from}" not found '
                                   f'in results. Check identifiers and order')

            link_table = all_sub[sub_desc_from]['table']

            link_field = sub_table[1].get(link_table)
            if not link_field:
                raise RuntimeError(f'Link "{visor.table}"=>"{link_table}" '
                                   f'not found')

            link_field = link_field[0]

            q.where(link_field, Q.Any(sub_resp[sub_desc_from]['ids']))

        if where is not None:
            for field, value in where.items():
                q.where(self._to_db(field), value)

        if order is not None:
            for field, order in order.items():
                q.order(self._to_db(field), order)

        items = list(await self._conn.fetch(q))

        primary_key_field = visor.primary_key

        def make_key(key):
            if isinstance(key, UUID):
                key = str(key)
            return key

        def make_item(item: dict):
            return {self._from_db(k): v for k, v in item.items()}

        add_data = {make_key(item[primary_key_field]): make_item(item)
                    for item in items}

        return add_data, link_field

    async def insert(self, *, visor, insert, user):
        idx = 1
        args = ()
        own_fields = []
        own_values = []

        for key, value in insert.items():
            own_fields.append(f'"{self._to_db(key)}"')
            own_values.append(f'${idx}')
            args += (value,)
            idx += 1

        last_updated_by_field = self._to_db('last_updated_by')
        own_fields.append(f'"{last_updated_by_field}"')
        own_values.append(f'${idx}')
        args += (user.user_id,)
        idx += 1

        own_fields_str = ', '.join(own_fields)
        own_values_str = ', '.join(own_values)

        primary_key = visor.primary_key

        query = f'INSERT INTO {visor.table} ({own_fields_str}) ' \
            f'VALUES ({own_values_str}) RETURNING "{primary_key}"'
        result = await self._conn.fetchrow(query, *args)

        return result

    async def update(self, *, visor, update, user):
        query, upd = update

        idx = 1
        args = ()
        fields = []
        for key, value in upd.items():
            fields.append(f'"{self._to_db(key)}" = ${idx}')
            args += (value,)
            idx += 1

        last_updated_by_field = self._to_db('last_updated_by')
        fields.append(f'"{last_updated_by_field}" = ${idx}')
        args += (user.user_id,)
        idx += 1

        value_fields = ', '.join(fields)

        primary_key_field = visor.primary_key

        args += (query[primary_key_field],)
        primary_key_idx = idx

        idx += 1
        query_str = f'UPDATE {visor.table} SET {value_fields} ' \
            f'WHERE "{primary_key_field}" = ${primary_key_idx}'
        await self._conn.execute(query_str, *args)

    async def delete(self, *, visor: 'GrozaVisor', delete, user: GrozaUser):
        q = (
            Q.delete()
             .from_(visor.table)
             .where(visor.primary_key, delete[visor.primary_key])
        )
        await self._conn.execute(q)

    def transaction(self):
        return self._conn.transaction()

    def raw_conn(self):
        return self._conn

    async def __aenter__(self):
        return _PostgresConn(await self._conn.__aenter__(), logger=self._log)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._conn.__aexit__(exc_type, exc_val, exc_tb)

    def _from_db(self, field):
        return self._field_transformer.from_db(field)

    def _to_db(self, field):
        return self._field_transformer.to_db(field)


class _AsyncpgSessionProxy:
    def __init__(self, conn, log):
        self._conn = conn
        self._log = log

    async def __aenter__(self):
        return AsyncpgSession(conn=await self._conn.__aenter__(), log=self._log)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._conn.__aexit__(exc_type, exc_val, exc_tb)


class AsyncpgStorage(GrozaStorage):
    def __init__(self, dsn, notifications):
        self._log = build_logger('SESSION')
        self._backend = _PostgresBackend(dsn)

        self._notif_conn = None

        self._notifications = notifications

    async def connect(self):
        await self._backend.connect()

        self._notif_conn = await self._backend.pool.acquire()

        for model in self._get_visors():
            await self._notif_conn.add_listener(model.table, self._notify)

    def session(self):
        return _AsyncpgSessionProxy(conn=self._backend.acquire(), log=self._log)

    def release(self, session: AsyncpgSession):
        self._backend.release(session)

    async def start_tables(self):
        audit_table = 'groza_audit'
        audit_table_seq = f'{audit_table}_id_seq'
        changed_by_field = f'updatedBy'

        async with self._backend.pool.acquire() as conn:
            await conn.execute(f'CREATE SEQUENCE IF NOT EXISTS '
                               f'"{audit_table_seq}"')

            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{audit_table}" (
                   "audit_id" int8 PRIMARY KEY,
                   "{changed_by_field}" int8 NOT NULL,
                   "int_key" int8,
                   "time" timestamp NOT NULL,
                   "operation" bpchar NOT NULL,
                   "table" varchar NOT NULL,
                   "var_key" varchar,
                   "o" hstore,
                   "n" hstore
                );
            """)

            for model in self._get_visors():
                lower_table = model.table.lower()
                audit_prefix_table = f'{lower_table}_audit'
                audit_table_func = f'{audit_prefix_table}_func'
                audit_table_trigger = f'{audit_prefix_table}_trigger'

                last_updated_by_field = f'last_updated_by'

                primary_key_field = model.primary_key

                use_int_key = f'OLD."{primary_key_field}"' if False else 'NULL'
                use_var_key = f'OLD."{primary_key_field}"' if True else 'NULL'

                use_int_key_new = f'NEW."{primary_key_field}"' if False else 'NULL'
                use_var_key_new = f'NEW."{primary_key_field}"' if True else 'NULL'

                table = model.table
                data_table = f"{table}"

                await conn.execute(f"""
                    CREATE OR REPLACE FUNCTION "{audit_table_func}"() RETURNS TRIGGER AS 
                    $$ 
                    DECLARE 
                     r record;
                     oldh hstore;
                     o hstore := hstore('');
                     n hstore := hstore('');
                    BEGIN 
                     IF (TG_OP = 'DELETE') THEN
                       INSERT INTO "{audit_table}" SELECT nextval('{audit_table_seq}'), OLD."{last_updated_by_field}", {use_int_key}, now(), 'D', '{table}', {use_var_key}, hstore(OLD), hstore('');
                       PERFORM pg_notify('{data_table}', OLD."{primary_key_field}"::text);
                       RETURN OLD;
                     ELSIF (TG_OP = 'UPDATE') THEN
                       oldh = hstore(OLD);
                       FOR r IN SELECT * FROM EACH(hstore(NEW)) 
                       LOOP 
                         IF (oldh->r.key != r.value) THEN 
                           o = o || ('"' || r.key || '" => "' || (oldh->r.key) || '"')::hstore;
                           n = n || ('"' || r.key || '" => "' || r.value || '"')::hstore;
                         END IF;
                       END LOOP; 
                       INSERT INTO "{audit_table}" SELECT nextval('{audit_table_seq}'), NEW."{last_updated_by_field}", {use_int_key}, now(), 'U', '{table}', {use_var_key}, o, n;
                       PERFORM pg_notify('{data_table}', NEW."{primary_key_field}"::text);
                       RETURN NEW;
                     ELSIF (TG_OP = 'INSERT') THEN
                       INSERT INTO "{audit_table}" SELECT nextval('{audit_table_seq}'), NEW."{last_updated_by_field}", {use_int_key_new}, now(), 'A', '{table}', {use_var_key_new}, hstore(''), hstore(NEW);
                       PERFORM pg_notify('{data_table}', NEW."{primary_key_field}"::text);
                       RETURN NEW;
                     END IF;
                     RETURN NULL;
                    END; 
                    $$ language plpgsql; 
                """)

                await conn.execute(f"""
                    DROP TRIGGER IF EXISTS "{audit_table_trigger}" ON "{table}"
                """)

                await conn.execute(f"""
                   CREATE TRIGGER "{audit_table_trigger}"
                       AFTER INSERT OR UPDATE OR DELETE ON "{table}"
                       FOR EACH ROW EXECUTE PROCEDURE "{audit_table_func}"()
                """)

    def _notify(self, conn, pid, channel, message):
        obj_id = message
        # print(conn, pid, channel, message)
        self._notifications.put_nowait((pid, channel, obj_id))

    @classmethod
    def _get_visors(cls) -> Iterable[GrozaVisor]:
        # visors: GrozaVisors
        return groza_visors.get().visor_values()
