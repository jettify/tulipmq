import asyncio
from unittest import TestCase
import asyncio_redis
from tulipmq.consumer import Consumer

__author__ = 'nick'

class RedisQueueTestCase(TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.conn = None

    def tearDown(self):
        self.loop.close()
        self.loop = None

    @asyncio.coroutine
    def connect(self):
        conn = yield from asyncio_redis.Connection.create(
                host='localhost', port=6379, db=5, loop=self.loop)
        return conn

    def test_basic(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cons = Consumer(conn)
        self.loop.run_until_complete(go())