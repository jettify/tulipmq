import asyncio
from tulipmq.utils import get_key
from .settings import DEFAULT_Q


class RedisQueue(object):
    """
    Queue implementation using redis list data structure.

    Redis has nice list data structure, witch naturaly can be used
    as underlying structure for queue implementation. RPUSH and BLPOP
    used to add and remove data from redis list. BLPOP blocks connection
    until someone add something to the list

    Basic example
    >>>q = RedisQueue(redis_conn, q_name)
    >>>q.put('data')
    Deferred()
    >>>q.get()
    Deferred()
    """

    def __init__(self, conn, qname=None):
        """
        Init queue.

        :param dbconn: redis connection or connection pool
        :type value: ```txredisapi.ConnectionPool```
        :param qname: redis list to use as queue underlying structure
        :type qname: ```str```
        """
        self._conn = conn
        self._qname = qname or DEFAULT_Q

    @property
    def qname(self):
        """
        Returns queue name, which used as list name in redis.

        :return: name of the queue
        :rtype: ``str``
        """
        return self._qname

    @asyncio.coroutine
    def empty(self):
        """
        Check weather queue is empty or not.

        :return: deferred with bool value
        :rtype: ``Deferred``
        """
        size = yield from self.size()
        return size == 0


    @asyncio.coroutine
    def size(self):
        """
        Check current size of queue.

        :return: deferred with size ```int``` of the queue
        :rtype: ``Deferred``
        """
        size = yield from self._conn.llen(get_key(self.qname))
        return size

    @asyncio.coroutine
    def put(self, value):
        """
        Put string value to the tail of the redis list.

        :param value: data to store in queue
        :type value: ``str``
        """
        #TODO: docstring
        key = get_key(self.qname)
        yield from self._conn.rpush(key, [value])

    @asyncio.coroutine
    def get(self):
        """
        Dequeue data from head of list, if redis list is empty, BLPOP command
        waits for new data and only then fires deferred

        :return: deferred with data fetched from redis list
        :rtype: ``Deferred``
        """
        key = get_key(self.qname)
        data = yield from self._conn.blpop([key,], 0)
        return data.value

