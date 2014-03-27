import gevent
from testing.client import TestClient


class TestTest(object):
    def test_yeild_loop(self):
        client = TestClient('1.2.3.4:3030')
        called_yield = False

        try:
            timeout = gevent.Timeout(0.1)
            timeout.start()
            gevent.sleep(0.1)
            called_yield = True
            client.yield_loop()
            assert False, 'Expected timeout.'
        except gevent.Timeout, error:
            assert error is timeout, 'Wrong timeout caught'
        finally:
            timeout.cancel()

        assert called_yield
