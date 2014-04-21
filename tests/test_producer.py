import asyncio
import json
import asyncio_redis

from unittest import TestCase
from tulipmq import connect_producer
from tulipmq.producer import Producer


class ProducerTestCase(TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.conn = None

    def tearDown(self):
        self.loop.close()
        self.loop = None

    def test_rq_properties(self):

        ref_msg = {
            'id': "42",
            'namespace': 'test_ns',
            'action': 'do_task',
            'data': {"a": 7, "b": "eight"}
            }

        @asyncio.coroutine
        def go():
            tmq = yield from connect_producer(
                'test_ns', 'test', host='localhost', port=6379, db=5, loop=self.loop)
            yield from tmq.do_task(id="42", a=7, b="eight")
            qmsg = yield from tmq._queue.get()
            self.assertDictEqual(ref_msg, json.loads(qmsg))

        self.loop.run_until_complete(go())
