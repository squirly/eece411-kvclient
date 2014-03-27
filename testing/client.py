import gevent
from kvclient import KeyValueClient
from kvclient.base import KVCommands
import struct


class TestClient(KeyValueClient):
    yield_loop = lambda: gevent.sleep(0)

    def send_incomplete_command(self, command_value, key, value=None):
        with self.connect() as socket:
            if command_value is KVCommands.PUT:
                if socket.sendall(struct.pack('b32s512s', command_value, key, value)) is None:
                    self.error_handler(self.receive_bytes(1), key)

            elif command_value is KVCommands.GET:
                if socket.sendall(struct.pack('b32s', command_value, key)) is None:
                    self.error_handler(self.receive_bytes(1), key)
                    return self.receive_bytes(1024)

            elif command_value is KVCommands.DELETE:
                pass
            else:
                pass
