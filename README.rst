TulipMQ
=======

**TulipMQ** is super simple message queue based on  redis_ database and
asyncio_ (PEP-3156/tulip) framework. Created for study purposes, not
battle tested or used in production.

.. _redis: http://redis.io/
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html

TODO:
-----
1. add message format validation
2. proper exception handling
3. get rid of redis flushdb in tests