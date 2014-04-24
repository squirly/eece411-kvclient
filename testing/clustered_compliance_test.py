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
    shutdown_fraction = 0.5
    shutdown_timeout = 30
    rest_time = 15

    def set_shutdown_fraction(self, shutdown_fraction):
        self.shutdown_fraction = shutdown_fraction
        return self

    def set_shutdown_timeout(self, shutdown_timeout):
        self.shutdown_timeout = shutdown_timeout
        return self

    def set_rest_time(self, rest_time):
        self.rest_time = rest_time
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

    def put_test_set(self, keys):
        return [
            self.test_put(keys, self.value_persist),
        ]

    def get_test_set(self, keys):
        return [
            self.test_get(keys, self.value_persist),
        ]

    def run_test(self):
        self.running_nodes = self.resolved_addresses.values()

        if len(self.running_nodes) == 0:
            self.results.append(TextTestResult('Test failed', 'No available nodes.'))
            return

        self.reset_test()
        self.run_compliance_test(self.large_test_set, 7)

        self.reset_test()
        self.run_compliance_test(self.put_test_set, 0)
        self.run_node_shutdown()
        self.rest()
        self.test_down_nodes()
        if len(self.running_nodes) == 0:
            raise TestError(TextTestResult())
        self.run_compliance_test(self.get_test_set, 0)

        self.reset_test()
        self.run_compliance_test(self.quick_test_set, 1)
        self.rest()

        self.reset_test()
        self.run_compliance_test(self.large_test_set, 7)

    def reset_test(self):
        super(ClusteredComplianceTest, self).reset_test()
        self.value_three = random.random()
        self.value_persist = random.random()
        super(ClusteredComplianceTest, self).reset_test()

    def run_compliance_test(self, test_set, rests=0):
        l.info('Running compliance test.')
        with time_it() as time:
            self.results.append(ClusteredTestResult(
                map(self.get_node_name, self.running_nodes),
                super(ClusteredComplianceTest, self).run_compliance_test(test_set)
            ))
        self.results.append(TextTestResult('Last test took:', str(time.span.total_seconds()-rests*self.rest_time) + 's'))
        l.info('Compliance test finished in ' + str(time.span.total_seconds()-rests*self.rest_time) + 's')

    def rest(self):
        gevent.sleep(self.rest_time)

    def get_client(self, addresses=None):
        if addresses is None:
            addresses = self.running_nodes
        return super(ClusteredComplianceTest, self).get_client(addresses)

    def run_node_shutdown(self):
        self.setup_node_shutdown()
        shutdown_count = math.floor(len(self.running_nodes)*self.shutdown_fraction)
        while len(self.shutdown_nodes) + len(self.failed_nodes) < shutdown_count:
            self.shutdown_node()
        self.summarize_node_shutdown()

    def setup_node_shutdown(self):
        self.shutdown_results = []
        self.shutdown_failed_results = []
        self.shutdown_nodes = []
        self.failed_nodes = set()

    def shutdown_node(self):
        address = random.choice(self.running_nodes)
        node_name = self.get_node_name(address)
        timeout = Timeout(self.shutdown_timeout, TestTimeout())
        timeout.start()
        try:
            self.get_client(address).shutdown()
            self.shutdown_results.append(TextTestResult(node_name, 'Shutdown signal sent.'))
            self.shutdown_nodes.append(address)
            l.info('Shutdown node ' + address)
        except TestTimeout:
            self.failed_nodes.add(address)
            self.shutdown_failed_results.append(TextTestResult(node_name, 'Node timed-out when signaled to shutdown.'))
            l.exception('Shutdown timeout on node ' + address)
        except KeyValueError, error:
            self.failed_nodes.add(address)
            self.shutdown_failed_results.append(TextTestResult(node_name, 'Node returned error when signaled to shutdown. ' + str(error)))
            l.exception('Shutdown error (' + hex(error.ERROR_CODE) + ') on node ' + address)
        except IOError, error:
            self.failed_nodes.add(address)
            self.shutdown_failed_results.append(TextTestResult(node_name, 'Connection error on node when signaled to shutdown. ' + str(error)))
            l.exception('Shutdown connection error on node ' + address)
        except Exception, error:
            self.failed_nodes.add(address)
            self.shutdown_failed_results.append(TextTestResult(node_name, 'Node does not want to shutdown. ' + str(error)))
            l.exception('Shutdown unknown error on node ' + address)
        finally:
            timeout.cancel()

        self.running_nodes.remove(address)

    def summarize_node_shutdown(self):
        if len(self.shutdown_failed_results) > 0:
            self.results.extend(self.shutdown_failed_results)
        else:
            self.results.append(TextTestResult(
                'Node shutdown',
                'All nodes accepted the shutdown command.'
            ))
        if len(self.shutdown_results) > 0:
            self.results.append(ClusteredTestResult(
                map(self.get_node_name, self.shutdown_nodes),
                self.shutdown_results
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
