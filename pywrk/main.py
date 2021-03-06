import asyncio
from collections import deque
from concurrent.futures import ProcessPoolExecutor

from pywrk.request import async_run
from pywrk.util import (assign_conn, parse_duration, parse_header, analysis,
                        count_req_sec)


async def main(url, works, headers, timeout, duration, connections, method):
    loop = asyncio.get_event_loop()
    data = deque()
    result = {}
    spend = 0

    connection_num = assign_conn(connections, works)
    duration = parse_duration(duration)
    if headers:
        headers = parse_header(headers)

    with ProcessPoolExecutor(max_workers=works) as exc:
        for i in range(works):
            result[i] = loop.run_in_executor(exc, run, i, url, headers,
                                             connection_num[i], timeout,
                                             duration, method)
    for _, v in result.items():
        cache_data, cache_spend = await v
        data += cache_data
        spend += cache_spend
    spend = spend / works

    num, status_result, avg_spend, max_spend = analysis(data)
    print(f"{num} requests in {spend}")
    print()
    print("NOTE: Count the entire asyncio time instead of request time")
    print(f"AVG: {avg_spend}")
    print(f"MAX: {max_spend}")
    print()
    print(f"Request/sec: {count_req_sec(num, spend)}")
    for k, v in status_result.items():
        print(f"status code: {k}; send: {v} requests")


def run(num, url, headers, connections, timeout, duration, method):
    r = asyncio.run(
        async_run(num, url, headers, timeout, connections, duration, method))
    return r
