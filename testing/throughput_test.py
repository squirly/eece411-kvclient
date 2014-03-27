from greenlet import GreenletExit
from logging import getLogger
import random
import string
from collections import Counter
import gevent
from gevent import Greenlet
from kvclient.exceptions import SystemOverloadError
from testing.base import IndependentNodesTest, ClusteredNodesTest, Test

l = getLogger(__name__)


class BadReturnedValueError(Exception):
    pass


class ThroughputResult(object):
    ADDRESS_KEY = 'node'

    def __init__(self, address, values):
        self.address = address
        self.values = values

    def to_dict(self):
        return {
            self.ADDRESS_KEY: self.address,
            'data': self.values,
            'x_axis': 'Client Count',
            'y_axis': 'Requests/s',
        }


class ClusteredThroughputResult(ThroughputResult):
    ADDRESS_KEY = 'nodes'


class ThroughputTestBase(Test):
    client_counts = [1, 8, 16, 32]
    test_length_seconds = 60

    def set_client_counts(self, client_counts):
        self.client_counts = client_counts
        return self

    def set_test_length_seconds(self, test_length_seconds):
        self.test_length_seconds = test_length_seconds
        return self

    def run_test(self):
        self.test_running = False
        node_result = []

        for client_count in self.client_counts:
            requests = Counter()
            client_creator = self.make_client_creator(requests)
            client_prefixes = [str(i) for i in range(100000, 100000 + client_count)]
            clients = map(client_creator, client_prefixes)
            self.test_running = True
            map(lambda g: g.start(), clients)
            gevent.sleep(self.test_length_seconds)
            self.test_running = False
            gevent.killall(clients)
            r = {
                'Client Count': client_count,
                'Requests/s': {k: v/self.test_length_seconds for k, v in requests.items()},
            }
            l.info(r)
            node_result.append(r)
        return node_result

    def make_client_creator(self, counter):
        def client_creator(test_prefix):
            return Greenlet(self.client_runner, test_prefix, counter)
        return client_creator

    def client_runner(self, test_prefix, counter):
        while self.test_running:
            try:
                self.run_key(test_prefix)
                counter['success'] += 1
            except GreenletExit:
                return
            except SystemOverloadError:
                counter['overload'] += 1
                gevent.sleep()
            except Exception:
                counter['failed'] += 1
                gevent.sleep()

    def run_key(self, test_prefix):
        key = test_prefix + self.get_key()
        value = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
        self.get_client().put(key, value)
        if value != self.get_client().get(key)[0:10]:
            raise BadReturnedValueError()
        self.get_client().delete(key)

    def get_key(self, length=26):
        return ''.join(random.choice(string.ascii_lowercase) for _ in range(length))


class ThroughputTest(ThroughputTestBase, IndependentNodesTest):
    result_class = ThroughputResult


class ClusteredThroughputTest(ThroughputTestBase, ClusteredNodesTest):
    result_class = ClusteredThroughputResult
