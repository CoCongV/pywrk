import async_timeout
from collections import deque, defaultdict
from concurrent.futures import ProcessPoolExecutor
from timeit import default_timer as timer
import typing

import aiohttp
import orjson


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
    spend = 0

    connection_num = assign_conn(connections, works)
    duration = parse_duration(duration)
    if headers:
        headers = parse_header(headers)
    with ProcessPoolExecutor(max_workers=works) as exc:
        for i in range(works):
            result[i] = exc.submit(run, i, url, headers, connection_num[i],
                                   timeout, duration, method)

    for _, v in result.items():
        cache_data, cache_spend = v.result()
        data += cache_data
        spend += cache_spend
    spend = spend / works

    num, status_result = analysis(data)
    print(f"{num} requests in {spend}")
    print(f"Request/sec: {count_req_sec(num, spend)}")


def analysis(data: deque):
    num = 0
    status_result = defaultdict(int)

    for i in data:
        status_result[i[0]] += 1
        num += 1
    return num, status_result


def count_req_sec(all_req, duration):
    return all_req / duration


def run(num, url, headers, connections, timeout, duration, method):
    import asyncio
    import uvloop

    uvloop.install()
    return asyncio.run(
        async_run(num, url, headers, timeout, connections, duration, method))


async def async_run(num, url, headers, timeout, connection_num, duration,
                    method):
    import asyncio
    queue = CustomDeque()
    client = await create_client(headers, timeout, connection_num, method)
    method_func = getattr(client, method)

    try:
        start = timer()
        async with async_timeout.timeout(duration):
            while True:
                await asyncio.sleep(0)
                asyncio.create_task(request(method_func, url, queue))
    except asyncio.TimeoutError:
        queue.close()
    finally:
        spend = timer() - start
        await client.close()
        return queue, spend


async def request(client, url: str, queue: CustomDeque):
    start = timer()
    if queue.is_close:
        return
    try:
        async with client(url) as response:
            queue.append((response.status, timer() - start))
    except aiohttp.client_exceptions.ClientConnectionError:
        pass


async def create_client(headers, timeout, connections, method):
    connector = aiohttp.TCPConnector(limit=connections)
    client = aiohttp.ClientSession(connector=connector,
                                   json_serialize=orjson.dumps)
    return client


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
