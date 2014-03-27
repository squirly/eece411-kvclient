from contextlib import contextmanager
from datetime import datetime
from testing.client import TestClient
from kvclient.base import KVCommands
from testing.base import ClusteredNodesTest, TextTestResult, ClusteredTestResult, TestTimeout, TestError
from testing.mass_test import MassTestBase
import gevent
from gevent.timeout import Timeout
from kvclient.exceptions import KeyValueError
from logging import getLogger
import math
import random

l = getLogger(__name__)


class TimeMeasure(object):
    def __init__(self, start):
        self.start = start
        self.end = None

    def finish(self, end):
        self.end = end

    @property
    def span(self):
        if self.end:
            return self.end - self.start
        return datetime.now() - self.start


@contextmanager
def time_it():
    measurer = TimeMeasure(datetime.now())
    yield measurer
    measurer.finish(datetime.now())


class ClusteredComplianceTest(MassTestBase, ClusteredNodesTest):
    shutdown_timeout = 30
    rest_time = 30
    shutdown_fraction = 0.5

    def set_shutdown_fraction(self, shutdown_fraction=0.5):
        self.shutdown_fraction = shutdown_fraction
        return self

    def large_test_set(self, keys):
        return [
            self.test_put(keys, self.value_one),
            self.rest(),
            self.test_get(keys, self.value_one),
            self.rest(),
            self.test_put(keys, self.value_two),
            self.rest(),
            self.test_get(keys, self.value_two),
            self.rest(),
            self.test_delete(keys),
            self.rest(),
            self.test_failure(keys, KVCommands.GET),
            self.rest(),
            self.test_failure(keys, KVCommands.DELETE),
            self.rest(),
        ]

    def quick_test_set(self, keys):
        return [
            self.test_put(keys, self.value_three),
            self.rest(),
            self.test_get(keys, self.value_three),
        ]

    def run_test(self):
        self.shutdown_nodes = []
        self.value_three = random.random()
        self.running_nodes = self.resolved_addresses.values()

        if len(self.running_nodes) == 0:
            self.results.append(TextTestResult('Test failed', 'No available nodes.'))
            return
        self.run_compliance_test(self.large_test_set, 7)
        self.run_node_shutdown()
        self.rest()
        self.test_down_nodes()
        if len(self.running_nodes) == 0:
            raise TestError(TextTestResult())
        self.run_compliance_test(self.quick_test_set, 1)
        self.rest()
        self.run_compliance_test(self.large_test_set, 7)

    def run_compliance_test(self, test_set, rests):
        l.info('Running compliance test.')
        with time_it() as time:
            self.results.append(ClusteredTestResult(
                map(self.get_node_name, self.running_nodes),
                super(ClusteredComplianceTest, self).run_compliance_test(test_set)
            ))
        self.results.append(TextTestResult('Last test took:', str(time.span.total_seconds()) + 's'))
        l.info('Compliance test finished in ' + str(time.span.total_seconds()-rests*self.rest_time) + 's')

    def rest(self):
        gevent.sleep(self.rest_time)

    def get_client(self, addresses=None):
        if addresses is None:
            addresses = self.running_nodes
        return super(ClusteredComplianceTest, self).get_client(addresses)

    def run_node_shutdown(self):
        results = []
        failed_results = []
        shutdown_count = math.floor(len(self.running_nodes)*self.shutdown_fraction)
        self.failed_nodes = set()
        while len(self.shutdown_nodes) + len(self.failed_nodes) < shutdown_count:
            address = random.choice(self.running_nodes)
            timeout = Timeout(self.shutdown_timeout, TestTimeout())
            timeout.start()
            try:
                TestClient(address).shutdown()
                results.append(TextTestResult(self.get_node_name(address), 'Shutdown signal sent.'))
                self.running_nodes.remove(address)
                self.shutdown_nodes.append(address)
                l.info('Shutdown node ' + address)
            except TestTimeout:
                self.failed_nodes.add(address)
                failed_results.append(TextTestResult(self.get_node_name(address), 'Node timed-out when signaled to shutdown.'))
                l.exception('Shutdown node ' + address)
            except KeyValueError, error:
                self.failed_nodes.add(address)
                failed_results.append(TextTestResult(self.get_node_name(address), 'Node returned error when signaled to shutdown. ' + str(error)))
                l.exception('Shutdown node ' + address)
            except Exception, error:
                self.failed_nodes.add(address)
                failed_results.append(TextTestResult(self.get_node_name(address), 'Node does not want to shutdown. ' + str(error)))
                l.exception('Shutdown node ' + address)
            finally:
                timeout.cancel()

        if len(failed_results) > 0:
            self.results.append(ClusteredTestResult(
                map(self.get_node_name, self.failed_nodes),
                failed_results
            ))
        else:
            self.results.append(TextTestResult(
                'Node shutdown',
                'All nodes accepted the shutdown command.'
            ))
        if len(results) > 0:
            self.results.append(ClusteredTestResult(
                map(self.get_node_name, self.shutdown_nodes),
                results
            ))
        else:
            self.results.append(TextTestResult(
                'Node shutdown',
                'No nodes were successfully shutdown.'
            ))

    def test_down_nodes(self):
        results = []
        for node in self.shutdown_nodes:
            timeout = Timeout(self.shutdown_timeout)
            timeout.start()
            try:
                TestClient(node).put('test', self.get_value('test'))
                results.append(TextTestResult(self.get_node_name(node), 'Node still responding to requests.'))
            except:
                pass
            finally:
                timeout.cancel()
        if len(results) > 0:
            self.results.append(ClusteredTestResult(
                map(self.get_node_name, self.shutdown_nodes),
                results
            ))
        else:
            self.results.append(TextTestResult(
                'Check shutdown nodes',
                'All shutdown nodes not responding to requests.'
            ))
