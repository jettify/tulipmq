import asyncio
from tulipmq.utils import get_key
from .settings import DEFAULT_Q


class RedisQueue(object):
    """Queue implementation using redis list data structure.

    Redis has nice list data structure, witch naturally can be used
    as underlying structure for queue implementation. RPUSH and BLPOP
    used to add and remove data from redis list. BLPOP blocks connection
    until someone add something to the list."""

    def __init__(self, conn, qname=None):
        """
        Init queue.

        :param conn: ``asyncio_redis.Connection`` redis connection
        :param qname: redis list to use as queue underlying structure
        :type qname: ```str``` name for queue (or redis list)
        """
        self._conn = conn
        self._qname = qname or DEFAULT_Q

    @property
    def qname(self):
        """
        Returns queue name, which used as list name in redis.
        :return: ``str`` name of the queue
        """
        return self._qname

    @asyncio.coroutine
    def empty(self):
        """Check weather queue is empty or not.
        :return: ``bool`` """
        size = yield from self.size()
        return size == 0


    @asyncio.coroutine
    def size(self):
        """Coroutine to check current size of queue.

        :return: ``int`` size of the queue"""
        size = yield from self._conn.llen(get_key(self.qname))
        return size

    @asyncio.coroutine
    def put(self, value):
        """Put string (serialized json, for example) value to the
        tail of the redis list.

        :param value: data to store in queue"""
        key = get_key(self.qname)
        yield from self._conn.rpush(key, [value])

    @asyncio.coroutine
    def get(self):
        """Dequeue data from head of list, if redis list is empty,
        BLPOP command waits for new data.

        :return: ``str`` serialized vdata fetched from redis list
        """
        key = get_key(self.qname)
        data = yield from self._conn.blpop([key,], 0)
        return data.value

