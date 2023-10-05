from __future__ import annotations
import socket
from queue import Queue
from typing import Literal, Optional, Union
import functools
import asyncio

def yield_when_done(f, *args, **kwargs):
    @functools.wraps(f)
    async def wrapper(self: NotRedisConn, *args, **kwargs):
        await f(self, *args, **kwargs)
        await self.yield_to_pool()
    return wrapper


class NotRedisConnPool():
    """
    TODO: write about usage (use .initialise)
    """
    def __init__(self, host: str, port: int, size: int = 8):
        self.addr = (host, port)
        self._size = size
        self._sem = asyncio.Semaphore(size)
        self._connections: Queue[NotRedisConn] = Queue()

    async def initialise(self):
        for _ in range(self._size):
            self._connections.put(await self._create_new_conn())

    async def _create_new_conn(self):
        """
        Create a new connection to the server.
        Note that this does NOT add the connection to the pool.
        """
        return await NotRedisConn(*self.addr, self).initialise()

    async def get_conn(self) -> NotRedisConn:
        await self._sem.acquire()
        return self._connections.get()

    async def yield_conn(self, conn: NotRedisConn):
        self._connections.put(conn)
        self._sem.release()

class NotRedisConn():
    """
    TODO: write about usage (use .initialise)
    """
    def __init__(self, host: str, port: int, pool: NotRedisConnPool):
        self.socket = socket.create_connection((host, port))
        self.pool = pool
        self.host = host
        self.port = port

    async def initialise(self) -> NotRedisConn:
        # self.socket = socket.create_connection((host, port))
        self.read, self.write = await asyncio.open_connection(self.host, self.port)
        return self

    def _send(self, data: Union[bytes, bytearray]):
        # The first 4 bytes of the data is the length of the data
        data = len(data).to_bytes(4, byteorder='big') + data
        self.write.write(data)

    async def recv(self) -> bytes:
        # size = self.socket.recv(4)
        size = await self.read.readexactly(4)
        return await self.read.readexactly(int.from_bytes(size))

    async def recv_str(self) -> str:
        return (await self.recv()).decode()

    @yield_when_done
    async def get(self, key: str) -> Optional[str]:
        msg = f'GET {key}'
        self._send(msg.encode())
        return await self._recv_with_arg()

    @yield_when_done
    async def delete(self, key: str) -> Optional[str]:
        msg = f'DEL {key}'
        self._send(msg.encode())
        return await self._recv_with_arg()

    @yield_when_done
    async def set(self, key: str, value: str) -> Optional[str]:
        msg = f'SET {key} {value}'
        self._send(msg.encode())
        return await self._recv_with_arg()

    @yield_when_done
    async def ping(self) -> Literal["PONG"]:
        msg = 'PING'
        self._send(msg.encode())
        retval = await self.recv_str()
        assert retval == 'PONG'
        return retval

    @yield_when_done
    async def echo(self, msg: str) -> Optional[str]:
        msg = f'ECHO {msg}'
        self._send(msg.encode())
        return await self._recv_with_arg()

    @yield_when_done
    async def clear(self) -> Literal["CLR"]:
        msg = 'CLR'
        self._send(msg.encode())
        retval = await self.recv_str()
        assert retval == 'CLR'
        return retval

    async def _recv_with_arg(self) -> Optional[str]:
        prev_val = await self.recv_str()
        return (
            prev_val[4:] # f'CMD {arg}' -> f'{arg}'
            if prev_val != '(nil)'
            else None
        )


    async def yield_to_pool(self):
        await self.pool.yield_conn(self)

    def close(self):
        self.write.close()

