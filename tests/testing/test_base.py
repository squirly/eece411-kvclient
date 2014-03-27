import socket
from testing.base import Test, TestTimeout, TextTestResult, TestResult, ClusteredTestResult, IndependentNodesTest, ClusteredNodesTest
import gevent
import mock
import pytest


class TestTest(object):
    def test_resolve_address(self):
        class MockSocket:
            @staticmethod
            def gethostbyname(domain):
                assert domain == 'my_domain.com'
                return '143.56.7.4'

        test = Test(None)

        with mock.patch('testing.base.socket', MockSocket):
            address = test.resolve_address('my_domain.com:54123')

        assert address == '143.56.7.4:54123'

    def test_resolve_invalid_address(self):
        test = Test(None)

        with pytest.raises(ValueError):
            test.resolve_address('i_have_no_port.com')

    def test_resolve_bad_address(self):
        class MockSocket:
            @staticmethod
            def gethostbyname(domain):
                assert domain == 'my_domain.com'
                raise socket.error()

        test = Test(None)

        with mock.patch('testing.base.socket', MockSocket):
            with pytest.raises(socket.error):
                test.resolve_address('my_domain.com:54123')

    def test_initial_test(self):
        test = Test(None)

        mock_client = mock.Mock()
        mock_client.return_value = mock_client
        mock_client.put = mock.Mock()

        with mock.patch('testing.base.TestClient', mock_client):
            test.initial_test('1.2.3.4:8043')

        mock_client.assert_called_once_with('1.2.3.4:8043')
        mock_client.put.assert_called_once_with('key', 'value')

    def test_failed_initial_test(self):
        test = Test(None)

        mock_client = mock.Mock(side_effect=socket.error())

        with mock.patch('testing.base.TestClient', mock_client):
            with pytest.raises(socket.error):
                test.initial_test('1.2.3.4:8043')

    def test_timeout_initial_test(self):
        test = Test(None)
        test.TIMEOUT = 0

        mock_client = lambda address: gevent.sleep(0.1)

        with mock.patch('testing.base.TestClient', mock_client):
            with pytest.raises(TestTimeout):
                test.initial_test('1.2.3.4:8043')


class TestTestResult(object):
    def test_to_dict(self):
        result = TestResult('my_node.com:3121', [
            TextTestResult('a_command', 'the command\'s result'),
            TextTestResult('a second command', 'the second command\'s result'),
        ])

        assert result.to_dict() == {
            'node': 'my_node.com:3121',
            'results': [
                ['a_command', 'the command\'s result'],
                ['a second command', 'the second command\'s result'],
            ],
        }


class TestTextTestResult(object):
    def test_format_results(self):
        result = TextTestResult('my_node.com:3121', 'A message.')
        assert result.format_result() == ['my_node.com:3121', 'A message.']

    def test_to_dict(self):
        result = TextTestResult('my_node.com:3121', 'A message.')

        assert result.to_dict() == {
            'node': 'my_node.com:3121',
            'result': 'A message.',
        }


class TestClusteredTestResult(object):
    def test_to_dict(self):
        result = ClusteredTestResult('my_node.com:3121', [
            TextTestResult('a_command', 'the command\'s result'),
        ])

        assert result.to_dict() == {
            'nodes': 'my_node.com:3121',
            'results': [
                ['a_command', 'the command\'s result'],
            ],
        }


class TestIndependentNodesTest(TestTest):
    def test_get_client(self):
        tester = IndependentNodesTest(None)
        tester.address = 'my_test_address.com:3042'

        client = tester.get_client()

        assert client.location == 'my_test_address.com'
        assert client.port == 3042

    def test_run(self):
        pass


class TestClusteredNodesTest(TestTest):
    def test_get_client(self):
        tester = ClusteredNodesTest(None)
        addresses = {
            'a': '1.2.3.4:3041',
            'b': '2.3.4.5:3042',
            'c': '3.4.5.6:3043',
            'd': '4.5.6.7:3044'
        }
        tester.resolved_addresses = addresses

        class MockRandom(object):
            def __init__(self, addresses):
                self.choice_counter = 0
                self.addresses = addresses

            def choice(self, addresses):
                assert addresses == self.addresses.values()
                self.choice_counter += 1
                if self.choice_counter >= len(addresses):
                    self.choice_counter = 0
                return addresses[self.choice_counter]

        retrieved_locations = []
        retrieved_ports = []

        with mock.patch('testing.base.random', MockRandom(addresses)):
            for _ in range(8):
                client = tester.get_client()
                retrieved_locations.append(client.location)
                retrieved_ports.append(client.port)

        assert len(retrieved_locations) == 8
        assert set(retrieved_locations) == set(['1.2.3.4', '2.3.4.5', '3.4.5.6', '4.5.6.7'])

        assert len(retrieved_ports) == 8
        assert set(retrieved_ports) == set([3041, 3042, 3043, 3044])
