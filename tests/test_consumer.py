import asyncio
import json
from unittest.mock import patch
import asyncio_redis

from unittest import TestCase
from tulipmq.consumer import Consumer
from tulipmq.exceptions import TMQNoNamespaceHandler
from tulipmq.handler import BaseHandler


class TestHandlerClass(BaseHandler):

    namespace = 'test_ns'

    @asyncio.coroutine
    def do_task_handler(self, **kwargs):
        yield from asyncio.sleep(0, loop=self.loop)
        self.check()

    def check(self):
        print("Used as mock obj for tests")

def task_gen(consumer):
    for i in range(2):
        task = consumer.wait_for_data()
        yield task



class ConsumerTestCase(TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        # asyncio.set_event_loop(None)

        self.ref_msg = {
            'id': "42",
            'namespace': 'test_ns',
            'action': 'do_task',
            'data': {"a": 7, "b": "eight"}
            }

        self.ref_msg_false = {
            'id': "42",
            'namespace': 'NOT_test_ns',
            'action': 'do_task',
            'data': {"a": 7, "b": "eight"}
            }

    def tearDown(self):
        self.loop.close()
        self.loop = None

    @asyncio.coroutine
    def connect(self):
        conn = yield from asyncio_redis.Connection.create(
                host='localhost', port=6379, db=5, loop=self.loop)
        consumer_tmq = Consumer(conn, 1, 'test', loop=self.loop)
        return consumer_tmq


    def test_consumer_dispatch(self):
        TestHandlerClass.loop = self.loop
        @asyncio.coroutine
        def go():
            consumer_tmq = yield from self.connect()
            with patch.object(TestHandlerClass, 'check'):
                yield from consumer_tmq._dispatch(json.dumps(self.ref_msg))
                self.assertTrue(TestHandlerClass.check.called)

            yield from consumer_tmq._queue._conn.flushdb()

        self.loop.run_until_complete(go())

    def test_consumer_dispatch_no_handler(self):
        @asyncio.coroutine
        def go():
            consumer_tmq = yield from self.connect()
            with patch.object(TestHandlerClass, 'check'):
                try:
                    yield from consumer_tmq._dispatch(json.dumps(self.ref_msg_false))
                except TMQNoNamespaceHandler:
                    self.assertTrue(True)
        self.loop.run_until_complete(go())


    def test_consumer_cooperate(self):

        TestHandlerClass.loop = self.loop

        @asyncio.coroutine
        def go():
            consumer_tmq = yield from self.connect()
            yield from consumer_tmq._queue.put(json.dumps(self.ref_msg))
            yield from consumer_tmq._queue.put(json.dumps(self.ref_msg))
            with patch.object(TestHandlerClass, 'check'):
                tg = task_gen(consumer_tmq)
                yield from asyncio.wait(consumer_tmq.work(tg), loop=self.loop)
                self.assertEqual(TestHandlerClass.check.call_count, 2)
            yield from consumer_tmq._queue._conn.flushdb()

        self.loop.run_until_complete(go())
