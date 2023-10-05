from __future__ import annotations
import socket
import time
from queue import Queue
from typing import Literal, Optional, Union
from threading import Semaphore
import functools
import asyncio

def yield_when_done(f, *args, **kwargs):
    @functools.wraps(f)
    async def wrapper(self: NotRedisConn, *args, **kwargs):
        f(self, *args, **kwargs)
        await self.yield_to_pool()
    return wrapper


class NotRedisConnPool():
    async def __init__(self, host: str, port: int, size: int = 8):
        self.addr = (host, port)
        self._size = size
        self._sem = asyncio.Semaphore(size)
        self._connections: Queue[NotRedisConn] = Queue()
        for _ in range(size):
            self._connections.put(self._create_new_conn())

    def _create_new_conn(self):
        """
        Create a new connection to the server.
        Note that this does NOT add the connection to the pool.
        """
        conn = NotRedisConn(*self.addr, self)
        return conn

    async def get_conn(self) -> NotRedisConn:
        await self._sem.acquire()
        return self._connections.get()

    async def yield_conn(self, conn: NotRedisConn):
        self._connections.put(conn)
        self._sem.release()

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

    async def yield_to_pool(self):
        await self.pool.yield_conn(self)

    def close(self):
        self.socket.close()


