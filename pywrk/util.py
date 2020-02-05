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

    for i in data:
        status_result[i[0]] += 1
        num += 1
    return num, status_result


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
