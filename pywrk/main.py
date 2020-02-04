import asyncio
import async_timeout
from collections import deque, defaultdict
from multiprocessing import Pool
import typing

import httpx
import uvloop

uvloop.install()


class CustomDeque(deque):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_close = False

    def close(self):
        self.is_close = True

    def append(self, x):
        if self.is_close:
            return
        super().append(x)


class Duration:
    s = 1
    m = 60
    h = m * 60
    d = h * 24


def main(url, works, headers, timeout, duration, connections, method):
    data = deque()
    result = {}

    connection_num = assign_conn(connections, works)
    duration = parse_duration(duration)
    if headers:
        headers = parse_header(headers)
    # data += run(url, headers, connection_num[0], timeout, duration, method)
    with Pool(works) as p:
        for i in range(works):
            result[i] = p.apply_async(run,
                                      args=[
                                          i, url, headers, connection_num[i],
                                          timeout, duration, method
                                      ])
        p.close()
        p.join()
        for _, v in result.items():
            data += v.get()
    print(count_req(data))


def count_req(data: deque):
    result = defaultdict(int)
    for i in data:
        result[i] += 1
    return result


def run(num, url, headers, connections, timeout, duration, method):
    return asyncio.run(
        async_run(num, url, headers, timeout, connections, duration, method))


async def async_run(num, url, headers, timeout, connection_num, duration,
                    method):
    print(num)
    queue = CustomDeque()
    client = await create_client(headers, timeout, connection_num, method)
    try:
        async with async_timeout.timeout(duration):
            while True:
                await asyncio.sleep(0)
                asyncio.create_task(request(client, url, queue))
    except asyncio.TimeoutError:
        queue.close()
    finally:
        return queue


async def request(client, url: str, queue: CustomDeque):
    if queue.is_close:
        return
    try:
        r: httpx.Response = await client(url)
        queue.append(r.status_code)
    except httpx.exceptions.NetworkError:
        queue.append("timeout")
    except httpx.exceptions.ProxyError as e:
        queue.append(e.response.status_code)


async def create_client(headers, timeout, connections, method):
    pool_limits = httpx.PoolLimits(soft_limit=connections,
                                   hard_limit=connections)
    client = httpx.AsyncClient(headers=headers,
                               timeout=timeout,
                               pool_limits=pool_limits)
    return getattr(client, method)


def parse_header(header_str: str) -> typing.Dict[typing.AnyStr, typing.AnyStr]:
    headers = {}
    header_str_list = header_str.split(";")
    for h in header_str_list:
        content = h.split(":")
        headers[content[0]] = content[1]
    return headers


def parse_duration(d: str) -> (int, str):
    t = d[-1]
    t = getattr(Duration, t)
    if not t:
        raise AttributeError(f"{t} is not in duration")
    v = int(d[:-1]) * t
    return v


def assign_conn(connections, works):
    works_assign = []
    each_conn = connections // works
    for _ in range(works - 1):
        works_assign.append(each_conn)
    works_assign.append(connections - each_conn * (works - 1))
    return works_assign
