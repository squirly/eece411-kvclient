import gevent
from testing.throughput_test import ThroughputTestBase
from mock import MagicMock


class MockThroughputTest(ThroughputTestBase):
    def get_client(self):
        client = MagicMock()
        client.get.return_value = '1'
        return client


class TestThroughputTest(object):

    def test_create(self):
        test = MockThroughputTest(['1.2.3.4', '4.3.2.1'])
        test.client = MagicMock()

        test. \
            set_test_length_seconds(0.1). \
            set_client_counts([1, 2, 3, 4])

        results = test.run_test()

        results.get()