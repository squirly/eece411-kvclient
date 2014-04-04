from testing.mass_test import MassTestBase
from kvclient.base import KeyValueClient
from mock import MagicMock, Mock, patch


class MockMassTest(MassTestBase):
    def get_client(self):
        return self.client

    def reset_test(self):
        pass

class TestMassTestBase(object):

    def test_create(self):
        test = MockMassTest(['1.2.3.4', '4.3.2.1'])
        test.client = MagicMock()
        test.keys = range(0, 8)
        test.value_one = 1
        test.value_two = 2

        test. \
            set_key_count(8). \
            set_concurrency(2). \
            set_timeout(1)

        test.run_test()

        calls = test.client.get.call_args_list
        args_list = [arguments[0][0] for arguments in calls]

        assert set(args_list) == set(test.keys)
        assert test.client.get.call_count == 16
        assert test.client.put.call_count == 16
        assert test.client.delete.call_count == 8

