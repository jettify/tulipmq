import asyncio
from unittest import TestCase
import asyncio_redis
from tulipmq.rq import RedisQueue


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
        rq = RedisQueue(conn, 'test')
        return rq, conn

    def test_rq_properties(self):

        @asyncio.coroutine
        def go():
            rq, conn = yield from self.connect()
            qsize = yield from rq.size()
            is_empry = yield from rq.empty()

            self.assertEqual(rq.qname, 'test')
            self.assertEqual(qsize, 0)
            self.assertTrue(is_empry)

            # yield from conn.flushdb()

        self.loop.run_until_complete(go())

    def test_rq_put_get(self):

        @asyncio.coroutine
        def go():
            rq, conn = yield from self.connect()
            yield from rq.put("one")
            yield from rq.put("two")

            # one = yield from rq.get()
            # two = yield from rq.get()
            #
            # self.assertEqual(one, 'one')
            # self.assertEqual(two, 'two')

            # yield from conn.flushdb()

        self.loop.run_until_complete(go())