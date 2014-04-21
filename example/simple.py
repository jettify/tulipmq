import asyncio
import asyncio_redis
from tulipmq.consumer import Consumer


@asyncio.coroutine
def connect():
    conn = yield from asyncio_redis.Connection.create(
            host='localhost', port=6379, db=5)
    return conn

@asyncio.coroutine
def main(f):

    import ipdb; ipdb.set_trace()
    conn = yield from connect()
    consumer = Consumer(conn, qname='test')
    yield from asyncio.wait(consumer.work())
    yield from asyncio.sleep(10)
    return f.done()


f = asyncio.Future()
loop = asyncio.get_event_loop()

loop.run_until_complete(main(f))
loop.run_forever()