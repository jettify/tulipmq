import json
from tulipmq import settings


class Job:
    """
    Container for job's metadata.

    Metadata could be accessed using standard dot notation,
    like ``job.namespace``, ``job.data`` or  ``job.action``
    """


    def __init__(self, job_str):
        self._raw_data = job_str
        self._job_obj = json.loads(self._raw_data)

    def __getattr__(self, item):
        if item in self._job_obj:
            return self._job_obj[item]
        msg = "{0}  object has no attribute {1}"
        raise AttributeError(msg.format(type(self).__name__, item))

    def raw(self):
        return self._raw_data


def get_key(queue_name):
    """
    Helper method to construct key for redis structure used
    in queue module

    :param queue_name: name of the queue
    :type queue_name: ```str```
    """
    return u'{0}:{1}'.format(settings.REDIS_LIST_PREFIX, queue_name)
