from testing.mass_test import MassTestBase


class TestMassTestBase(object):
    def test_create(self):
        test = MassTestBase(['1.2.3.4', '4.3.2.1'])
        test. \
            set_key_count(128). \
            set_concurrency(16). \
            set_timeout(0)
