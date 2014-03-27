from logging import getLogger
from kvclient.base import KVCommands
from testing.client import TestClient
from kvclient.exceptions import KeyValueError, InvalidKeyError
from testing.base import TextTestResult, IndependentNodesTest
from gevent import Timeout
import random
import string

l = getLogger(__name__)


class TestTimeout(Exception):
    pass


class test_case(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, func):
        def test_wrapper(this, *args, **kwargs):
            def run_test():
                try:
                    timeout = Timeout(1, TestTimeout())
                    result = func(this, *args, **kwargs)
                    timeout.cancel()
                    yield 'success' if result is None else result
                except KeyValueError, e:
                    timeout.cancel()
                    yield e.ERROR_CODE
                except TestTimeout:
                    timeout.cancel()
                    yield 'timeout'
                finally:
                    timeout.cancel()
            return EdgeTestOperationResult(self.name, run_test())
        return test_wrapper


class EdgeTest(IndependentNodesTest):
    def __init__(self, addresses):
        self.addresses = addresses
        self.result = None
        self.resolved_address = None

    def key_generator(self, key_range):
        for _ in range(key_range):
            return random.choice(string.ascii_lowercase)

    def test_set(self):
        return [
            self.test_empty(self.key_generator(32), 0x00),
            self.test_empty(None, KVCommands.GET),
            self.test_empty(self.key_generator(16), KVCommands.GET),
            self.test_short_value(self.key_generator(32), KVCommands.PUT, '12341234'),
        ]

    def run_on_node(self):
        return self.test_set()

    def test_empty(self, key, command):
        kvclient = TestClient(self.address)
        try:
            kvclient.send_command(command, key)
            return TextTestResult(command, 'failed')
        except InvalidKeyError:
            pass

    def test_short_value(self, key, command, value):
        kvclient = TestClient(self.address)
        try:
            kvclient.send_incomplete_command(command, key, value)
            return TextTestResult(command, 'failed')
        except InvalidKeyError:
            pass


class EdgeTestOperationResult(object):
    def __init__(self, command, result):
        self.command = command
        self.result = result


class EdgeTestResult(object):
    def __init__(self, address, commands):
        self.node_address = address
        self.commands = commands

    def to_dict(self):
        return {
            'node': self.node_address,
            'results': self.commands.command + self.result,
        }
