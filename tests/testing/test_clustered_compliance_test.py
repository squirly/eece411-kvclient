from testing.clustered_compliance_test import ClusteredComplianceTest
from mock import MagicMock


class MockClusteredComplianceTest(ClusteredComplianceTest):
    def get_client(self):
        return self.client

    def rest(self):
        pass

    def reset_test(self):
        pass


class TestClusteredComplianceTest(object):
    def test_create(self):
        test = MockClusteredComplianceTest(['node1.com:9000', 'node2.com:9000', 'node3.com:9000', 'node4.com:9000'])
        test.client = MagicMock()
        test.results = []
        test.value_one = 1
        test.value_two = 2
        test.value_three = 3
        test.value_persist = 1
        test.keys = range(0, 8)
        test.resolved_addresses = {'node1.com:9000': '1.2.3.4:9000',
                                   'node2.com:9000': '4.3.2.1:9000',
                                   'node3.com:9000': '2.3.4.1:9000',
                                   'node4.com:9000': '3.4.1.2:9000'}

        test.set_timeout(0) \
            .set_key_count(4) \
            .set_concurrency(2) \
            .set_rest_time(0) \
            .set_shutdown_timeout(0)

        test.run_test()
