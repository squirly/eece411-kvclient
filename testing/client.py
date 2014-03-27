import gevent
from kvclient import KeyValueClient


class TestClient(KeyValueClient):
    yield_loop = gevent.sleep
