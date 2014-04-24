from testing.client import TestClient
from gevent import Timeout, socket
from logging import getLogger
import random

l = getLogger(__name__)


class TestTimeout(Exception):
    pass


class TestError(Exception):
    def __init__(self, test_result):
        self.test_result = test_result


class Test(object):
    TIMEOUT = 10

    def __init__(self, addresses, **extra):
        self.addresses = addresses
        self.extra = extra

    def resolve_address(self, address):
        address_components = address.split(':')
        if len(address_components) != 2:
            raise ValueError('The server address must contain a port.')
        address_components[0] = socket.gethostbyname(address_components[0])
        return ':'.join(address_components)

    def initial_test(self, address):
        try:
            timeout = Timeout(self.TIMEOUT, TestTimeout('The server timed out on the first command.'))
            timeout.start()
            TestClient(address).put('key', 'value')
        finally:
            timeout.cancel()

    def get_client(self):
        raise NotImplementedError()


class TestResult(object):
    node_key = 'node'
    result_key = 'results'

    def __init__(self, name, result):
        self.name = name
        self.result = result

    def to_dict(self):
        return {
            self.node_key: self.name,
            self.result_key: map(lambda c: c.format_result(), self.result),
        }


class TextTestResult(TestResult):
    result_key = 'result'

    def format_result(self):
        return [self.name, self.result]

    def to_dict(self):
        return {
            self.node_key: self.name,
            self.result_key: self.result,
        }


class ClusteredTestResult(TestResult):
    node_key = 'nodes'


class IndependentNodesTest(Test):
    result_class = TestResult

    def run(self):
        results = []
        for address in self.addresses:
            try:
                self.address = self.resolve_address(address)
                self.initial_test(self.address)
                node_result = self.run_test()
                results.append(self.result_class(address, node_result))
            except Exception, error:
                l.exception('Test against node ' + address + ' failed')
                results.append({
                    'node': address,
                    'error': str(error),
                })

        return results

    def get_client(self):
        return TestClient(self.address)

    def run_test(self):
        raise NotImplementedError()


class ClusteredNodesTest(Test):
    result_class = ClusteredTestResult

    def run(self):
        self.results = []
        self.resolve_addresses()

        test_nodes = self.resolved_addresses.keys()
        try:
            results = self.run_test()
            if results is not None:
                self.results.append(self.result_class(test_nodes, results))
        except Exception, error:
            l.exception('Test failed.')
            result = getattr(error, 'test_result', None)
            if result is None:
                result = {
                    'nodes': test_nodes,
                    'error': str(error),
                }
            self.results.append(result)

        return self.results

    def resolve_addresses(self):
        self.resolved_addresses = {}

        for address in self.addresses:
            try:
                resolved_address = self.resolve_address(address)
                self.initial_test(resolved_address)
                self.resolved_addresses[address] = resolved_address
            except Exception, error:
                result = getattr(error, 'test_result', None)
                if result is None:
                    result = {
                        'node': address,
                        'error': str(error),
                    }
                self.results.append(result)

    def get_node_name(self, node):
        return self.resolved_addresses.keys()[self.resolved_addresses.values().index(node)]

    def get_client(self, addresses=None):
        if addresses is None:
            addresses = self.resolved_addresses.values()
        return TestClient(random.choice(addresses))

    def run_test(self):
        raise NotImplementedError()
