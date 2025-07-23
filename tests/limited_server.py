import asyncio
from collections import deque
from datetime import timedelta, datetime


class ServerRateLimit(Exception):
    pass


class LimitedServer:
    def __init__(self, rate: int, window: timedelta):
        self.rate = rate
        self.window = window

        self.in_deque = deque()

    async def run(self, *args, **kwargs):
        now = datetime.now()
        # remove expired timestamps
        while len(self.in_deque) > 0 and now - self.in_deque[0] > self.window:
            self.in_deque.popleft()

        if len(self.in_deque) > self.rate:
            raise ServerRateLimit()

        self.in_deque.append(now)
        await asyncio.sleep(0.1)
        return f'server response on: {args=}, {kwargs=}'
