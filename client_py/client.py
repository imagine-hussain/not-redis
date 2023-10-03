import socket
import time
from queue import Queue
from typing import Literal, Optional, Union
import functools

def yield_when_done(f, *args, **kwargs):
    @functools.wraps(f)
    def wrapper(self: NotRedisConn, *args, **kwargs):
        f(self, *args, **kwargs)
        self.yield_to_pool()
    return wrapper


class NotRedisConnPool():
    def __init__(self, host: str, port: int, size: int = 8):
        self.addr = (host, port)
        self.connections: Queue[NotRedisConn] = Queue()
        # TODO: fill conn
        self.size = size

    def new_conn(self):
        conn = NotRedisConn(*self.addr, self)
        self.yield_conn(conn)
        return conn

    def get_conn(self) -> NotRedisConn:
        while (conn := self.connections.get()) is None:
            # dont actly sleep like this
            time.sleep(0.1)
        return conn

    def yield_conn(self, conn: NotRedisConn):
        self.connections.put(conn)


class NotRedisConn():

    def __init__(self, host: str, port: int, pool: NotRedisConnPool):
        self.socket = socket.create_connection((host, port))
        self.pool = pool


    def _send(self, data: Union[bytes, bytearray]):
        # The first 4 bytes of the data is the length of the data
        data = len(data).to_bytes(4, byteorder='big') + data
        self.socket.sendall(data)

    def recv(self) -> bytes:
        size = self.socket.recv(4)
        data = self.socket.recv(int.from_bytes(size))
        return data

    @yield_when_done
    def get(self, key: str) -> Optional[str]:
        msg = f'GET {key}'
        self._send(msg.encode())
        return self._recv_with_arg()

    @yield_when_done
    def delete(self, key: str) -> Optional[str]:
        msg = f'DEL {key}'
        self._send(msg.encode())
        return self._recv_with_arg()

    @yield_when_done
    def set(self, key: str, value: str) -> Optional[str]:
        msg = f'SET {key} {value}'
        self._send(msg.encode())
        return self._recv_with_arg()

    @yield_when_done
    def ping(self) -> Literal["PONG"]:
        msg = 'PING'
        self._send(msg.encode())
        retval = self.recv().decode()
        assert retval == 'PONG'
        return retval

    @yield_when_done
    def echo(self, msg: str) -> Optional[str]:
        msg = f'ECHO {msg}'
        self._send(msg.encode())
        return self._recv_with_arg()

    def _recv_with_arg(self) -> Optional[str]:
        return (
            prev_val[4:] # f'CMD {arg}' -> f'{arg}'
            if (prev_val := self.recv().decode()) != '(nil)'
            else None
        )

    def yield_to_pool(self):
        self.pool.yield_conn(self)

    def close(self):
        self.socket.close()


