from itertools import izip_longest
from logging import getLogger
import random
import string
from kvclient.exceptions import KeyValueError, InvalidKeyError
from gevent import Greenlet, GreenletExit, Timeout
import socket
from hashlib import sha512
from collections import Counter
from kvclient.base import KVCommands
from testing.base import TestTimeout, Test, IndependentNodesTest, ClusteredNodesTest


l = getLogger(__name__)


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


class test_case(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, func):
        def test_wrapper(this, keys, *args, **kwargs):
            def run_test():
                for key in keys:
                    timeout = Timeout(this.timeout, TestTimeout())
                    try:
                        timeout.start()
                        result = func(this, key, *args, **kwargs)
                        timeout.cancel()
                        yield 'success' if result is None else result
                    except KeyValueError, e:
                        timeout.cancel()
                        yield e.ERROR_CODE
                    except TestTimeout:
                        timeout.cancel()
                        yield 'timeout'
                    except GreenletExit:
                        return
                    except socket.error:
                        timeout.cancel()
                        yield 'connection_error'
                    except:
                        timeout.cancel()
                        l.exception('Error while testing.')
                        yield 'error'
                    finally:
                        timeout.cancel()
            return MassTestOperationResult(self.name, run_test())
        return test_wrapper


class MassTestBase(Test):
    VALUE_SIZE = 1024
    V_NONE = 0

    key_count = 16
    concurrent_clients = 1
    timeout = 5

    def set_key_count(self, key_count):
        self.key_count = key_count
        return self

    def set_concurrency(self, concurrency):
        self.concurrent_clients = concurrency
        return self

    def set_timeout(self, timeout):
        self.timeout = timeout
        return self

    def key_space_generator(self):
        for key_number in range(0, self.key_count):
            while len(self.keys) <= key_number:
                self.keys.append(''.join(random.choice(string.ascii_lowercase) for _ in range(32)))
            yield self.keys[key_number]

    def get_value(self, key, version=''):
        value = ''
        hasher = sha512()
        hasher.update(str(version))
        hasher.update(str(key))
        while len(value) <= self.VALUE_SIZE:
            hasher.update(value)
            value += hasher.digest()
        return value[:self.VALUE_SIZE]

    def test_set(self, keys):
        return [
            self.test_put(keys, self.value_one),
            self.test_get(keys, self.value_one),
            self.test_put(keys, self.value_two),
            self.test_get(keys, self.value_two),
            self.test_delete(keys),
            self.test_failure(keys, KVCommands.GET),
            self.test_failure(keys, KVCommands.DELETE),
        ]

    def run_test(self):
        return self.run_compliance_test(self.test_set)

    def run_compliance_test(self, test_set):
        self.value_one = random.random()
        self.value_two = random.random()
        self.keys = []

        key_groups = grouper(self.key_space_generator(), self.key_count/self.concurrent_clients)

        group_tests = []
        for keys in key_groups:
            group_tests.append(Greenlet(test_set, keys))

        for g in group_tests:
            g.start()

        result_set = []
        for g in group_tests:
            result_set.append(g.get())

        final_results = []

        for results in zip(*result_set):
            result = results[0]
            if result is None:
                continue
            for r in results[1:]:
                result += r
            final_results.append(result)

        return final_results

    @test_case('test_put')
    def test_put(self, key, version):
        kvclient = self.get_client()
        kvclient.put(key, self.get_value(key, version))

    @test_case('test_get')
    def test_get(self, key, expected_result):
        kvclient = self.get_client()
        response = kvclient.get(key)
        if response == self.get_value(key, expected_result):
            return None
        else:
            return 'failed'

    @test_case('test_delete')
    def test_delete(self, key):
        kvclient = self.get_client()
        kvclient.delete(key)

    @test_case('test_failure')
    def test_failure(self, key, command):
        kvclient = self.get_client()
        try:
            kvclient.send_command(command, key)
            return 'failed'
        except InvalidKeyError:
            pass


class MassTest(MassTestBase, IndependentNodesTest):
    pass


class ClusteredMassTest(MassTestBase, ClusteredNodesTest):
    pass


class MassTestOperationResult(object):
    def __init__(self, command, results):
        self.command = command
        self.result_count = Counter(results)

    def __add__(self, other):
        if isinstance(other, MassTestOperationResult) and self.command == other.command:
            return MassTestOperationResult(self.command, self.result_count + other.result_count)
        else:
            return super(MassTestOperationResult, self).__add__(other)

    def format_result(self):
        return [self.command, {str(a): b for a, b in self.result_count.items()}]
