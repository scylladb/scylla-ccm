from tests.ccmtest import DockerTester, FakeScyllaCluster
from ccmlib.scylla_manager import ScyllaManagerDocker


class TestScyllaManagerDocker(DockerTester):
    def test_simple(self):
        fake_cluster = FakeScyllaCluster()
        try:
            sm = ScyllaManagerDocker(fake_cluster)
            sm.start()
            out, err = sm.sctool("version")
            self.assertEqual(err, "expected no error")
            self.assertIn(out, "Client version", "client version is present")
            self.assertIn(out, "Server version", "server version is present")
        finally:
            sm.stop(False)
        with self.assertRaises(Exception):
            sm.sctool("version")

