import asyncpg

from pssq import Q
from groza.utils import build_logger


def _prepare(query, args):
    if isinstance(query, Q):
        query, args = query.end()
        return query, args

    return query, args


class _PostgresConn:
    def __init__(self, conn, logger):
        self.conn = conn
        self.logger = logger

    async def execute(self, query, *args):
        try:
            query, args = _prepare(query, args)
            await self.conn.execute(query, *args)
        except:
            self.logger.exception('Error in db execute: %s; %s'
                                  % (query, args))
            raise

    async def executemany(self, query, args):
        try:
            query, args = _prepare(query, args)
            await self.conn.executemany(query, args)
        except:
            self.logger.exception('Error in db executemany: %s; %s'
                                  % (query, args))
            raise

    async def fetch(self, query, *args):
        try:
            query, args = _prepare(query, args)
            return await self.conn.fetch(query, *args)
        except:
            self.logger.exception('Error in db fetch: %s; %s'
                                  % (query, args))
            raise

    async def fetchrow(self, query, *args):
        try:
            query, args = _prepare(query, args)
            return await self.conn.fetchrow(query, *args)
        except:
            self.logger.exception('Error in db fetchrow: %s; %s'
                                  % (query, args))
            raise

    async def fetchval(self, query, *args):
        try:
            query, args = _prepare(query, args)
            return await self.conn.fetchval(query, *args)
        except:
            self.logger.exception('Error in db fetchval: %s; %s'
                                  % (query, args))
            raise

    async def executemany(self, query, args):
        try:
            query, args = _prepare(query, args)
            await self.conn.executemany(query, args)
        except:
            self.logger.exception('Error in db executemany: %s; %s'
                                  % (query, args))
            raise

    async def executebatch(self, buildQuery, args, pagesize=5000, conn=None):
        if conn is None:
            conn = self.conn

        if not isinstance(args, list):
            args = list(args)

        npages = (len(args) + pagesize - 1) // pagesize

        for npage in range(npages):
            page = args[npage * pagesize:(npage + 1) * pagesize]

            query, queryArgs = buildQuery(page)

            try:
                await conn.execute(query, *queryArgs)
            except:
                self.logger.exception('Error in db query: %s; %s'
                                      % (query, queryArgs))
                raise

    def transaction(self):
        return self.conn.transaction()


class _PostgresPoolProxy:
    def __init__(self, conn, logger):
        self.conn = conn
        self.logger = logger

    async def __aenter__(self):
        return _PostgresConn(await self.conn.__aenter__(), logger=self.logger)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.conn.__aexit__(exc_type, exc_val, exc_tb)


class _PostgresBackend:
    def __init__(self, dsn: str):
        self.log = build_logger('DB')
        self.pool: asyncpg.Pool = None
        self.dsn: str = dsn

    async def connect(self):
        if self.pool:
            return

        self.pool = await asyncpg.create_pool(dsn=self.dsn)

    async def execute(self, query, *args):
        if isinstance(query, Q):
            query, args = query.end()
        try:
            self.log.debug('db execute: %s; %s', query, args)
            await self.pool.execute(query, *args)
        except:
            self.log.exception('Error in db execute: %s; %s'
                               % (query, args))
            raise

    async def executemany(self, query, args):
        if isinstance(query, Q):
            query, args = query.end()
        try:
            self.log.debug('db executemany: %s; %s', query, args)
            await self.pool.executemany(query, args)
        except:
            self.log.exception('Error in db executemany: %s; %s'
                               % (query, args))
            raise

    async def fetch(self, query, *args):
        if isinstance(query, Q):
            query, args = query.end()
        try:
            self.log.debug('db fetch: %s; %s', query, args)
            return await self.pool.fetch(query, *args)
        except:
            self.log.exception('Error in db fetch: %s; %s'
                               % (query, args))
            raise

    async def fetchrow(self, query, *args):
        if isinstance(query, Q):
            query, args = query.end()
        try:
            self.log.debug('db fetchrow: %s; %s', query, args)
            return await self.pool.fetchrow(query, *args)
        except:
            self.log.exception('Error in db fetchrow: %s; %s'
                               % (query, args))
            raise

    async def fetchval(self, query, *args):
        if isinstance(query, Q):
            query, args = query.end()
        try:
            self.log.debug('db fetchval: %s; %s', query, args)
            return await self.pool.fetchval(query, *args)
        except:
            self.log.exception('Error in db fetchval: %s; %s'
                               % (query, args))
            raise

    def acquire(self):
        return _PostgresPoolProxy(self.pool.acquire(), self.log)

    def release(self, conn):
        self.pool.release(conn)

    async def __aenter__(self):
        await self.connect()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.pool.__aexit__(exc_type, exc_val, exc_tb)
