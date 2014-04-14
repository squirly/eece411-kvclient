from testing.mass_test import MassTestBase
from testing.base import ClusteredNodesTest, ClusteredTestResult, TextTestResult, TestTimeout
import gevent
from testing.client import TestClient
from logging import getLogger
from contextlib import contextmanager
import random
from gevent.timeout import Timeout
from datetime import datetime
from kvclient.exceptions import KeyValueError

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


class RollingShutdownTest(MassTestBase, ClusteredNodesTest):
    time_between_shutdowns = 30
    rest_time = 15
    shutdown_fraction = 0.5
    key_count = 100
    shutdown_timeout = 30

    def set_time_between_shutdowns(self, time_between_shutdowns):
        self.time_between_shutdowns = time_between_shutdowns
        return self

    def set_rest_time(self, rest_time):
        self.rest_time = rest_time
        return self

    def set_shutdown_fraction(self, shutdown_fraction):
        self.shutdown_fraction = shutdown_fraction
        return self

    def set_key_count(self, key_count):
        self.key_count = key_count
        return self

    def set_shutdown_timeout(self, shutdown_timeout):
        self.shutdown_timeout = shutdown_timeout
        return self

    def run_test(self):
        self.shutdown_nodes = []
        self.running_nodes = self.resolved_addresses.values()

        if len(self.running_nodes) == 0:
            self.results.append(TextTestResult('Test failed', 'No available nodes.'))
            return

        self.run_compliance_test(self.put_test_set, self.rest_time)

        while (len(self.shutdown_nodes) / len(self.running_nodes)) < self.shutdown_fraction:
            self.run_node_shutdown()
            self.rest(self.time_between_shutdowns)

        self.run_compliance_test(self.get_test_set, self.rest_time)

    def run_compliance_test(self, test_set, rests):
        l.info('Running compliance test.')
        with time_it() as time:
            self.results.append(ClusteredTestResult(
                map(self.get_node_name, self.running_nodes),
                super(RollingShutdownTest, self).run_compliance_test(self.test_set)
            ))
        self.results.append(TextTestResult('Last test took:', str(time.span.total_seconds()-rests*self.rest_time) + 's'))
        l.info('Compliance test finished in ' + str(time.span.total_seconds()-rests*self.rest_time) + 's')

    def put_test_set(self, keys):
        return [
            self.test_put(keys, self.value_persist)
        ]

    def get_test_set(self, keys):
        return [
            self.test_get(keys, self.value_persist)
        ]

    def rest(self, time):
        gevent.sleep(time)

    def run_node_shutdown(self):
        results = []
        failed_results = []
        self.failed_nodes = set() ##

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
            self.shutdown_nodes.append(address)
            self.running_nodes.remove(address)
            failed_results.append(TextTestResult(self.get_node_name(address), 'Node timed-out when signaled to shutdown.'))
            l.exception('Shutdown node ' + address)
        except KeyValueError, error:
            self.failed_nodes.add(address)
            self.shutdown_nodes.append(address)
            self.running_nodes.remove(address)
            failed_results.append(TextTestResult(self.get_node_name(address), 'Node returned error when signaled to shutdown. ' + str(error)))
            l.exception('Shutdown node ' + address)
        except Exception, error:
            self.failed_nodes.add(address)
            self.shutdown_nodes.append(address)
            self.running_nodes.remove(address)
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






