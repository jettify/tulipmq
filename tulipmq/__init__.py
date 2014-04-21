import asyncio
import asyncio_redis
from tulipmq.producer import Producer

@asyncio.coroutine
def connect_producer(namespace, qname=None, **kwargs):
    conn = yield from asyncio_redis.Connection.create(**kwargs)
    tmq = Producer(conn, namespace, qname)
    return tmq
