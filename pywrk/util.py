from collections import defaultdict, deque
import typing


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


def analysis(data: deque):
    num = 0
    status_result = defaultdict(int)
    spend = 0
    max_spend = 0

    for i in data:
        status_result[i[0]] += 1
        if i[0] == 'timeout' or i[0] == 'network_error' or i[0] == 503:
            continue
        spend += i[1]
        if i[1] > max_spend:
            max_spend = i[1]
        num += 1

    avg_spend = spend / num if num else 0

    return num, status_result, readable_time(avg_spend), readable_time(
        max_spend)


def count_req_sec(all_req, duration):
    return all_req / duration


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


def readable_time(time):
    if time >= 1:
        return f"{time:.2f}s"
    else:
        return f"{(time * 1000):.2f}ms"
