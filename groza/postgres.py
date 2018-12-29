import asyncpg

from groza.q import Q
from groza.utils import build_logger


class PostgresDB:
    def __init__(self, dbname):
        self.log = build_logger("DB")
        self.pool: asyncpg.Pool = None

        self.host = "localhost"
        self.port = 5432
        self.dbname = dbname
        self.ssl = False
        self.user = "ridger"
        self.password = "ridger"

    async def connect(self):
        if self.pool:
            return

        self.pool = await asyncpg.create_pool(host=self.host, port=self.port, database=self.dbname, ssl=self.ssl,
                                              user=self.user, password=self.password)

    async def execute(self, query, *args):
        if isinstance(query, Q):
            query, args = query.END()
        try:
            self.log.debug("db execute: %s; %s", query, args)
            await self.pool.execute(query, *args)
        except:
            self.log.exception("Error in db execute: %s; %s" % (query, args))
            raise

    async def executemany(self, query, args):
        if isinstance(query, Q):
            query, args = query.END()
        try:
            self.log.debug("db executemany: %s; %s", query, args)
            await self.pool.executemany(query, args)
        except:
            self.log.exception("Error in db executemany: %s; %s" % (query, args))
            raise

    async def fetch(self, query, *args):
        if isinstance(query, Q):
            query, args = query.END()
        try:
            self.log.debug("db fetch: %s; %s", query, args)
            return await self.pool.fetch(query, *args)
        except:
            self.log.exception("Error in db fetch: %s; %s" % (query, args))
            raise

    async def fetchrow(self, query, *args):
        if isinstance(query, Q):
            query, args = query.END()
        try:
            self.log.debug("db fetchrow: %s; %s", query, args)
            return await self.pool.fetchrow(query, *args)
        except:
            self.log.exception("Error in db fetchrow: %s; %s" % (query, args))
            raise

    async def fetchval(self, query, *args):
        if isinstance(query, Q):
            query, args = query.END()
        try:
            self.log.debug("db fetchval: %s; %s", query, args)
            return await self.pool.fetchval(query, *args)
        except:
            self.log.exception("Error in db fetchval: %s; %s" % (query, args))
            raise
