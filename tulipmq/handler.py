from abc import abstractproperty
import asyncio


class MetaRegister(type):
    REGISTRY = {}

    def __new__(cls, name, bases, attrs):
        assert 'namespace' in attrs, "namespace is mandatory class attribute"

        new_cls = type.__new__(cls, name, bases, attrs)
        cls.REGISTRY[attrs['namespace']] = new_cls
        return new_cls


class BaseHandler(metaclass=MetaRegister):
    """

    """

    def __init__(self, job):
        self.job = job

    @abstractproperty
    def namespace(self):
        """
        Namespace identifier.
        """

    @asyncio.coroutine
    def _handle(self):
        """
        Method makes lookups for required action. Fetches action name from
        job metadata, adds '_handler' to this name, and try to lookup for
        attribute in this class and finally
        """
        action = '{0}_handler'.format(self.job.action)
        action_handler = getattr(self, action)
        yield from action_handler(**self.job.data)


class TestHandlerClass(BaseHandler):

    namespace = 'ns'

    @asyncio.coroutine
    def do_staff_handler(self, **kwargs):
        yield from asyncio.sleep(2)
        print(kwargs)