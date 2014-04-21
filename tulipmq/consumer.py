# coding: utf-8
import asyncio
from tulipmq import settings
from tulipmq.exceptions import TMQNoNamespaceHandler, TMQNoActionMethod
from tulipmq.handler import MetaRegister
from tulipmq.log import logger
from tulipmq.rq import RedisQueue
from tulipmq.utils import Job


class Consumer:
    """
    Consumer for simple message queue.
    """

    def __init__(self, conn, concurrency=1, qname=None, loop=None):
        self._qname = qname or settings.DEFAULT_Q
        self._queue = RedisQueue(conn, 'test')
        self.concurrency = concurrency
        self._loop = loop or asyncio.get_event_loop()
        self._is_running = True

    def wait_for_data(self):
        """Fetch data from queue ant pass it to next user."""
        task = asyncio.Task(self._queue.get(), loop=self._loop)
        return task

    def _create_generator(self):
        """
        Creates infinite generator. Method pulls data from queue and yields
        it for further processing.

        :return: ``iterable``generator to generate jobs
        """
        while self._is_running:
            task = self.wait_for_data()
            yield task

    @asyncio.coroutine
    def return_msg_back(self, msg):
        yield from self._queue.put(msg)

    @asyncio.coroutine
    def cooperate(self, iter, stop_future):

        for future in iter:
            msg_raw = yield from future
            logger.debug("Goe message from queue: {}".format(msg_raw))

            try:
                yield from self._dispatch(msg_raw)

            except (TMQNoNamespaceHandler, TMQNoActionMethod):
                log_msg = 'No handlers for msg: {}'.format(msg_raw)
                logger.error(log_msg)
            except Exception as e:
                # TODO: should be better way
                logger.error('Unhandled error in occurred: {},'
                             'wait {} sec until next job'.format(e, 5))
                yield self.return_msg_back(msg_raw)
                yield from asyncio.sleep(5, loop=self._loop)

        # notify work method that job done here
        stop_future.set_result(True)


    def _dispatch(self, msg_raw):
        job = Job(msg_raw)
        handle_class = MetaRegister.REGISTRY.get(job.namespace, None)
        if not handle_class:
            logger.warning("Namespace does not recognized, there is no"
                           "handler class for this namespace ?")
            raise TMQNoNamespaceHandler()

        handler = handle_class(job)
        yield from handler._handle()

    def work(self, generator=None):
        """Start listening and processing tasks"""
        job_gen = generator or self._create_generator()
        fs = []
        for i in range(self.concurrency):
            f = asyncio.Future(loop=self._loop)
            asyncio.async(self.cooperate(job_gen, f), loop=self._loop)
            fs.append(f)
        return fs
