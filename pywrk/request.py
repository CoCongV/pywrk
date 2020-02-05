import asyncio
from timeit import default_timer as timer

import aiohttp
import async_timeout
import httpx
import orjson

from pywrk.util import CustomDeque


async def async_run(num, url, headers, timeout, connection_num, duration,
                    method):
    # print(id(asyncio.get_event_loop()), num, asyncio.get_event_loop())
    queue = CustomDeque()
    client = await create_httpx_client(headers, timeout, connection_num,
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
                    httpx_req(method_func, url, queue, task_id, tasks))
                task_id += 1

    except asyncio.TimeoutError:
        queue.close()
    finally:
        spend = timer() - start
        await close_httpx_client(client)
        for _, v in tasks.items():
            v.cancel()
        return queue, spend


async def aiohttp_req(client, url: str, queue: CustomDeque, task_id: int,
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


async def create_aiohttp_client(headers, timeout, connections, method):
    timeout = aiohttp.ClientTimeout(total=timeout)
    connector = aiohttp.TCPConnector(limit=connections, ttl_dns_cache=300)
    client = aiohttp.ClientSession(connector=connector,
                                   json_serialize=orjson.dumps)
    return client


async def close_aiohttp_client(client):
    await client.close()


async def create_httpx_client(header, timeout, connections, method):
    timeout = httpx.Timeout(timeout=timeout)
    pool_limits = httpx.PoolLimits(soft_limit=connections,
                                   hard_limit=connections)
    client = httpx.AsyncClient(timeout=timeout, pool_limits=pool_limits)
    return client


async def close_httpx_client(client):
    await client.aclose()


async def httpx_req(client, url: str, queue: CustomDeque, task_id: int, tasks: dict):
    start = timer()
    if queue.is_close:
        return
    try:
        r = await client(url)
        queue.append((r.status_code, timer() - start))
    except httpx.exceptions.TimeoutException:
        queue.append(("timeout", ))
    except httpx.exceptions.NetworkError:
        pass
    finally:
        tasks.pop(task_id)
