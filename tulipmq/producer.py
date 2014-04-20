# coding: utf-8
import asyncio
import json
import uuid
import asyncio_redis
from tulipmq.rq import RedisQueue


class Producer:
    """
    Produce for simple queue.queue

    In order too use producer you need RedisQMixin setup first. Message
    Queue uses Redis as message storage, so anyway redis must be connected.

    >>>namespace_producer = Producer("some_namespace")
    >>>namespace_producer.do_smth_action(a=1, b=2)
    Deferred()

    following message will be constructed and inserted to redis list.

    ```{"id": 123456789,
    "some_namespace": "some_namespace",
    "action": "do_smth_action",
    "data": {"a": 1, "b": 2}```
    """

    serializer = json.dumps

    def __init__(self, conn, namespace, qname=None):
        """
        :param namespace: name of namespace for this handler container
        :type namespace: ``str``
        :param qname: queue to listen
        :type qname: ``str``
        """
        self.namespace = namespace
        self._queue = RedisQueue(conn, qname)

    def __getattr__(self, action):
        """
        Create method with required action, and passes it
        to client.
        """
        @asyncio.coroutine
        def add_job(self, **kw):
            msg = self.build_msg(action, **kw)
            yield from self.queue.put(msg)
        return add_job.__get__(self)

    def _build_msg(self, action, **kw):
        """
        Form job metadata as string with JSON data.
        """
        job_id = kw.pop('id', None) or str(uuid.uuid4())
        data = {
            'id': job_id,
            'namespace': self.namespace,
            'action': action,
            'data': kw
            }
        return self.serializer(data)


def connect_producer(namespace, qname=None, **kwargs):
    conn = yield from asyncio_redis.Connection.create(**kwargs)
    rq = Producer(conn, namespace, qname)
    return rq
