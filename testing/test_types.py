from testing.clustered_compliance_test import ClusteredComplianceTest
from testing.mass_test import MassTest, ClusteredMassTest
from testing.rolling_shutdown_test import RollingShutdownTest
from testing.throughput_test import ThroughputTest, ClusteredThroughputTest


def get_test_from_string(test_name, node, **extra):
    if test_name == 'simple_compliance':
        return MassTest(node, **extra). \
            set_key_count(5). \
            set_concurrency(1). \
            set_timeout(10)
    elif test_name == 'compliance':
        return MassTest(node, **extra). \
            set_key_count(20). \
            set_concurrency(2). \
            set_timeout(6)
    elif test_name == 'large_compliance':
        return MassTest(node, **extra). \
            set_key_count(100). \
            set_concurrency(5). \
            set_timeout(8)
    elif test_name == 'throughput':
        return ThroughputTest(node, **extra). \
            set_client_counts([1, 8, 16, 32, 64, 128, 256, 512]). \
            set_test_length_seconds(60)
    elif test_name == 'distributed_compliance':
        return ClusteredMassTest(node, **extra). \
            set_key_count(1024). \
            set_concurrency(128). \
            set_timeout(5)
    elif test_name == 'distributed_throughput':
        return ClusteredThroughputTest(node, **extra). \
            set_client_counts([1, 8, 16, 32, 64, 128, 256, 512]). \
            set_test_length_seconds(60)
    elif test_name == 'simple_phase_3':
        return ClusteredComplianceTest(node, **extra). \
            set_key_count(512). \
            set_concurrency(64). \
            set_timeout(10)
    elif test_name == 'phase_3':
        return ClusteredComplianceTest(node, **extra). \
            set_key_count(2048). \
            set_concurrency(64). \
            set_timeout(8)
    elif test_name == 'large_phase_3':
        return ClusteredComplianceTest(node, **extra). \
            set_key_count(10240). \
            set_concurrency(256). \
            set_timeout(5)
    elif test_name == 'simple_phase_4':
        return ClusteredComplianceTest(node, **extra). \
            set_key_count(4096). \
            set_concurrency(128). \
            set_timeout(10)
    elif test_name == 'phase_4':
        return ClusteredComplianceTest(node, **extra). \
            set_key_count(10240). \
            set_concurrency(128). \
            set_timeout(10)
    elif test_name == 'final_test':
        return RollingShutdownTest(node, **extra). \
            set_key_count(10240). \
            set_concurrency(128). \
            set_timeout(10). \
            set_time_between_shutdowns(30). \
            set_shutdown_timeout(30). \
            set_shutdown_fraction(0.5)
    else:
        raise Exception('No test named "' + test_name + '".')
