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
        test.value_one = 1
        test.value_two = 2
        test.keys = range(0, 8)
        test.results = []

        test.resolved_addresses = {'node1.com:9000': '1.2.3.4:9000',
                                   'node2.com:9000': '4.3.2.1:9000',
                                   'node3.com:9000': '2.3.4.1:9000',
                                   'node4.com:9000': '3.4.1.2:9000'}
        test.run_test()
