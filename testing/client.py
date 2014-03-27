import gevent
from kvclient import KeyValueClient


class TestClient(KeyValueClient):
    yield_loop = lambda self: gevent.sleep(0)
