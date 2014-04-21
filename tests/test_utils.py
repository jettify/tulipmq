import json
from unittest import TestCase
from tulipmq.utils import Job


class UtilsTestCase(TestCase):

    def setUp(self):

        self.ref_msg = {
            'id': '42',
            'namespace': 'test_ns',
            'action': 'do_task',
            'data': {'a': 7, 'b': 'eight'}
            }

    def test_job(self):
        job_str = json.dumps(self.ref_msg)

        job = Job(job_str)
        self.assertEqual(job.namespace, 'test_ns')
        self.assertEqual(job.action, 'do_task')
        self.assertEqual(job.id, '42')
        self.assertEqual(job.namespace, 'test_ns')
        self.assertEqual(job.raw(), job_str)

    def test_job_no_attr(self):
        job = Job(json.dumps(self.ref_msg))
        def clb(job):
            print(job.test)
        self.assertRaises(AttributeError, clb, job)

