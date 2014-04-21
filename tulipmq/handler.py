import asyncio
from tulipmq.exceptions import TMQNoNamespaceHandler


class MetaRegister(type):
    REGISTRY = {}

    def __new__(cls, name, bases, attrs):
        assert 'namespace' in attrs, "namespace is mandatory class attribute"
        new_cls = type.__new__(cls, name, bases, attrs)
        cls.REGISTRY[attrs['namespace']] = new_cls
        return new_cls


class BaseHandler(metaclass=MetaRegister):
    """XXX"""

    namespace = None

    def __init__(self, job):
        self.job = job
        assert self.namespace, "Namespace must be specified"

    @asyncio.coroutine
    def _handle(self):
        """
        Method makes lookups for required action. Fetches action name from
        job metadata, adds '_handler' to this name, and try to lookup for
        attribute in this class and finally
        """
        action = '{0}_handler'.format(self.job.action)
        action_handler = getattr(self, action, None)
        if not asyncio:
            msg = "No such handler: {}".format(action)
            raise TMQNoNamespaceHandler(msg)
        yield from action_handler(**self.job.data)


