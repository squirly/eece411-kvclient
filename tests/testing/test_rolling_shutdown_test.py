from testing.rolling_shutdown_test import RollingShutdownTest
from mock import MagicMock
from kvclient.base import KeyValueClient
from testing.client import TestClient

class MockRollingShutdownTest(RollingShutdownTest):
    def get_client(self):
        return self.client

    def reset_test(self):
        pass

    def rest(self, time):
        pass


class TestRollingShutdownTest(object):

    def test_create(self):
        test = MockRollingShutdownTest(['node1.com:9000', 'node2.com:9000', 'node3.com:9000', 'node4.com:9000'])

        test.client = MagicMock()

        test.value_one = 1
        test.value_two = 2
        test.value_three = 3
        test.value_persist = 1
        test.keys = range(0, 8)
        test.results = []

        test. \
            set_key_count(8). \
            set_rest_time(0). \
            set_shutdown_fraction(0.5). \
            set_shutdown_timeout(0). \
            set_time_between_shutdowns(0). \
            set_timeout(0)


        test.resolved_addresses = {'node1.com:9000': '1.2.3.4:9000',
                                   'node2.com:9000': '4.3.2.1:9000',
                                   'node3.com:9000': '2.3.4.1:9000',
                                   'node4.com:9000': '3.4.1.2:9000'}

        test.run_test()

        assert test.shutdown_fraction == 0.5

        assert len(test.running_nodes) == 2
        assert len(test.failed_nodes) == 2
        assert len(test.shutdown_nodes) == 2