# coding: utf-8
import asyncio
from tulipmq.handler import MetaRegister
from tulipmq.log import logger
from tulipmq.rq import RedisQueue
from tulipmq.utils import Job


class Consumer:
    """
    Consumer for simple message queue.
    """

    def __init__(self, conn, concurrency=3):
        self.queue = RedisQueue(conn, 'test')
        # self.coop = Cooperator(started=False)
        self.concurrency = concurrency
        self._is_running = False


    def wait_for_data(self):
        """
        Fetch data from queue ant pass it to next user.
        """
        task = asyncio.Task(self.queue.get())
        return task

    def _create_generator(self):
        """
        Creates infinite generator for tw.i.t.Cooperator. Method pulls data
        from queue (or any other deferred with data) and yields it for further
        processing.

        :return: generator to generate jobs
        :rtype: ``generator``
        """
        while True:
            task = self.wait_for_data()
            yield task


    @asyncio.coroutine
    def cooperate(self, iter):
        for future in iter:
            try:
                msg_raw = yield from future
                yield from self._dispatch(msg_raw)
            except Exception as e:
                logger.error("Unhandled error in occurred: {},"
                             "wait {} sec until next job".format(5, e))
                yield from asyncio.sleep(5)


    def _dispatch(self, msg_raw):
        """
        Method invokes required tw.i.t.Cooperator s and starts
        message processing.

        Cooperator used for controlling how many async jobs could be processed
        within this worker.
        """

        job = Job(msg_raw)
        handle_class = MetaRegister.REGISTRY.get(job.namespace, None)
        if not handle_class:
            print('no handle class')
            return
        handler = handle_class(job)
        yield from handler._handle()

    def work(self):
        job_gen = self._create_generator()
        for i in range(self.concurrency):
            asyncio.async(self.cooperate(job_gen))

        logger.debug("{} asyc processing routines have started.".format(
            (self.concurrency)))