import asyncio
from timeit import default_timer as timer
from typing import Union

from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiohttp.client_exceptions import ClientConnectionError
from aiohttp.helpers import sentinel
import async_timeout
import orjson
from yarl import URL

from pywrk.util import CustomDeque


async def async_run(num: int, url: str, headers: dict, timeout: Union[int,
                                                                      None],
                    connection_num: int, duration: int, method: str):
    url = URL(url)
    # print(id(asyncio.get_event_loop()), num, asyncio.get_event_loop())
    queue = CustomDeque()
    client = await create_aiohttp_client(headers, timeout, connection_num,
                                         method)
    method_func = getattr(client, method)
    tasks = {}
    task_id = 0

    try:
        start = timer()
        async with async_timeout.timeout(duration):
            while True:
                await asyncio.sleep(0)
                tasks[task_id] = asyncio.create_task(
                    aiohttp_req(method_func, url, queue, task_id, tasks))
                task_id += 1

    except asyncio.TimeoutError:
        queue.close()
    finally:
        spend = timer() - start
        await close_aiohttp_client(client)
        for _, v in tasks.items():
            v.cancel()
        return queue, spend


async def aiohttp_req(client: ClientSession.get, url: URL, queue: CustomDeque,
                      task_id: int, tasks: dict):
    start = timer()
    if queue.is_close:
        return
    try:
        async with client(url) as response:
            queue.append((response.status, timer() - start,))
            tasks.pop(task_id)
    except ClientConnectionError:
        pass


async def create_aiohttp_client(headers: dict, timeout: Union[int, None],
                                connections: int, method: str):
    timeout = ClientTimeout(sock_connect=timeout) if timeout else sentinel
    connector = TCPConnector(limit=connections, ttl_dns_cache=300)
    client = ClientSession(connector=connector,
                           json_serialize=orjson.dumps,
                           timeout=timeout)
    return client


async def close_aiohttp_client(client: ClientSession):
    await asyncio.sleep(0.25)
    await client.close()
