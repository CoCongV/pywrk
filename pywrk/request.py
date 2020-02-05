import asyncio
from timeit import default_timer as timer

import aiohttp
import async_timeout
import orjson

from pywrk.util import CustomDeque


async def async_run(num, url, headers, timeout, connection_num, duration,
                    method):
    # print(id(asyncio.get_event_loop()), num, asyncio.get_event_loop())
    queue = CustomDeque()
    client = await create_client(headers, timeout, connection_num, method)
    method_func = getattr(client, method)
    tasks = {}
    task_id = 0

    try:
        start = timer()
        async with async_timeout.timeout(duration):
            while True:
                await asyncio.sleep(0)
                tasks[task_id] = asyncio.create_task(
                    request(method_func, url, queue, task_id, tasks))
                task_id += 1

    except asyncio.TimeoutError:
        queue.close()
    finally:
        spend = timer() - start
        await client.close()
        for _, v in tasks.items():
            v.cancel()
        return queue, spend


async def request(client, url: str, queue: CustomDeque, task_id: int,
                  tasks: dict):
    start = timer()
    if queue.is_close:
        return
    try:
        async with client(url) as response:
            queue.append((response.status, timer() - start))
            tasks.pop(task_id)
    except aiohttp.client_exceptions.ClientConnectionError:
        pass


async def create_client(headers, timeout, connections, method):
    connector = aiohttp.TCPConnector(limit=connections)
    client = aiohttp.ClientSession(connector=connector,
                                   json_serialize=orjson.dumps)
    return client
