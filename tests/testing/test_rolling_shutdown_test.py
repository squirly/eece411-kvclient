from testing.rolling_shutdown_test import RollingShutdownTest
from mock import MagicMock, patch
from kvclient.exceptions import ServerFailureError


class MockRollingShutdownTest(RollingShutdownTest):
    def get_client(self, address=None):
        return self.client

    def reset_test(self):
        pass

    def rest(self, time=None):
        pass


class TestRollingShutdownTest(object):

    def test_create(self):
        test = MockRollingShutdownTest(
            ['node1.com:9000', 'node2.com:9000', 'node3.com:9000', 'node4.com:9000'],
            slice='2')

        test.client = MagicMock()

        test.client.shutdown.side_effect = [None, ServerFailureError(), None, None]

        test.value_one = 1
        test.value_two = 2
        test.value_three = 3
        test.value_persist = 1
        test.keys = range(0, 8)
        test.results = []

        test. \
            set_key_count(8). \
            set_concurrency(2). \
            set_rest_time(0). \
            set_shutdown_fraction(0.5). \
            set_shutdown_timeout(0.1). \
            set_time_between_shutdowns(0). \
            set_timeout(0)

        test.resolved_addresses = {'node1.com:9000': '1.2.3.4:9000',
                                   'node2.com:9000': '4.3.2.1:9000',
                                   'node3.com:9000': '2.3.4.1:9000',
                                   'node4.com:9000': '3.4.1.2:9000',
                                   'node5.com:9000': '3.5.1.2:9000',
                                   'node6.com:9000': '3.2.1.2:9000',
                                   'node7.com:9000': '3.8.1.2:9000',
                                   'node8.com:9000': '3.7.1.2:9000'}

        subprocess = MagicMock()
        subprocess.call.side_effect = [0, 0, 1, 0]
        with patch('testing.rolling_shutdown_test.subprocess', subprocess):
            test.run_test()

        assert test.shutdown_fraction == 0.5

        assert len(test.running_nodes) == 2
        assert len(test.failed_nodes) == 1
        assert len(test.shutdown_nodes) == 3
        assert len(test.killed_nodes) == 1

        assert test.client.shutdown.call_count == 4

        assert subprocess.call.call_count == 4

        assert test.client.get.call_count == 64
        assert test.client.put.call_count == 64
        assert test.client.delete.call_count == 24
