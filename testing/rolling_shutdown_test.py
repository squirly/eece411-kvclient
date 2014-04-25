import math
import random
from gevent import subprocess
from testing.clustered_compliance_test import ClusteredComplianceTest
from testing.base import TextTestResult, ClusteredTestResult
from logging import getLogger

l = getLogger(__name__)


class RollingShutdownTest(ClusteredComplianceTest):
    time_between_shutdowns = 30
    rest_time = 10

    def set_time_between_shutdowns(self, time_between_shutdowns):
        self.time_between_shutdowns = time_between_shutdowns
        return self

    def run_test(self):
        self.running_nodes = self.resolved_addresses.values()

        if len(self.running_nodes) == 0:
            self.results.append(TextTestResult('Test failed', 'No available nodes.'))
            return

        self.reset_test()
        self.run_compliance_test(self.test_set, self.rest_time)

        self.reset_test()
        self.run_compliance_test(self.put_test_set)
        self.run_node_shutdown()
        if len(self.running_nodes) == 0:
            self.results.append(TextTestResult('Test failed', 'No available nodes.'))
            return
        self.run_compliance_test(self.get_test_set)

        self.reset_test()
        self.run_compliance_test(self.test_set, self.rest_time)

        self.reset_test()
        self.run_compliance_test(self.put_test_set)
        self.run_hard_kill()
        if len(self.running_nodes) == 0:
            self.results.append(TextTestResult('Test failed', 'No available nodes.'))
            return
        self.run_compliance_test(self.get_test_set)

        self.reset_test()
        self.run_compliance_test(self.test_set, self.rest_time)

    def put_test_set(self, keys):
        return [
            self.test_put(keys, self.value_persist)
        ]

    def get_test_set(self, keys):
        return [
            self.test_get(keys, self.value_persist)
        ]

    def run_node_shutdown(self):
        self.setup_node_shutdown()
        shutdown_count = math.floor(len(self.running_nodes)*self.shutdown_fraction)
        while len(self.shutdown_nodes) + len(self.failed_nodes) < shutdown_count:
            self.shutdown_node()
            self.rest(self.time_between_shutdowns)
        self.summarize_node_shutdown()

    slices = {
        '2': 'ubc_EECE411_Snew',
        '3': 'ubc_EECE_S3',
        '4': 'ubc_EECE411_S4',
        '5': 'ubc_EECE411_S5',
        '6': 'ubc_EECE411_S6',
        '9': 'ubc_EECE411_S9',
    }

    def run_hard_kill(self):
        self.killed_nodes = []
        failed_nodes = []
        kill_results = []
        kill_failed_results = []

        shutdown_count = math.floor(len(self.running_nodes)*self.shutdown_fraction)
        while len(self.killed_nodes) + len(failed_nodes) < shutdown_count:
            address = random.choice(self.running_nodes)
            node_name = self.get_node_name(address)
            ssh_login = '{slice}@{node}'.format(
                slice=self.slices[self.extra['slice']],
                node=node_name.split(':')[0])
            ssh_command = "sudo killall -u {user} -9"
            results = set()
            successful_result_values = [0, 1, 255]
            for user in ['$USER', 'root']:
                command = ssh_command.format(user=user)
                l.info('Running command (' + command + ') on ' + ssh_login)
                try:
                    result = subprocess.call(['ssh', '-o', 'UserKnownHostsFile=/dev/null', '-o', 'StrictHostKeyChecking=no', ssh_login, command])
                except:
                    l.exception('Command failed')
                    result = -1
                if result not in successful_result_values:
                    results.add(result)
            if len(results) == 0:
                self.killed_nodes.append(address)
                kill_results.append(TextTestResult(node_name, 'All processes terminated.'))
            else:
                failed_nodes.append(address)
                kill_failed_results.append(TextTestResult(node_name, 'Processes not successfully terminated, error code(s) ' + (', '.join(map(str, results))) + '.'))
            self.running_nodes.remove(address)

        if len(kill_failed_results) > 0:
            self.results.extend(kill_failed_results)
        else:
            self.results.append(TextTestResult(
                'Node kill',
                'All nodes properly killed.'
            ))

        if len(kill_results):
            self.results.append(ClusteredTestResult(
                map(self.get_node_name, self.killed_nodes),
                kill_results
            ))
        else:
            self.results.append(TextTestResult(
                'Node kill',
                'No nodes were killed.'
            ))
