import os
from unittest import TestCase

import docker

class Tester(TestCase):

    def __init__(self, *argv, **kwargs):
        super(Tester, self).__init__(*argv, **kwargs)

    def setUp(self):
        pass

    def tearDown(self):
        if hasattr(self, 'cluster'):
            try:
                for node in self.cluster.nodelist():
                    self.assertListEqual(node.grep_log_for_errors(), [])
            finally:
                test_path = self.cluster.get_path()
                self.cluster.remove()
                if os.path.exists(test_path):
                    os.remove(test_path)


class FakeScyllaCluster:
    def __init__(self, *argv, **kwargs):
        super(FakeScyllaCluster, self).__init__(*argv, **kwargs)


class DockerTester(TestCase):

    def __init__(self, *argv, **kwargs):
        super(DockerTester, self).__init__(*argv, **kwargs)

        self.client = docker.from_env()
        self.container_count = len(self.client.containers.list())

    def setUp(self):
        pass

    def tearDown(self):
        got = len(self.client.containers.list())
        if self.container_count is not got:
            self.assertEqual(self.container_count, got, "Containers are not removed")
