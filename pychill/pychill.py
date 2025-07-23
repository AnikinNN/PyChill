import asyncio
import math
from collections import deque
from datetime import datetime, timedelta
from logging import getLogger, Logger

logger = getLogger(__name__)


class PyChillLimiterInvalidAlpha(Exception):
    ...


class PyChillLimiter:
    def __init__(
            self,
            rate: int,
            window: timedelta = timedelta(minutes=1),
            alpha: float = 0.95,
            logger: Logger = logger,
    ):
        self.rate = rate
        self.window = window
        self.alpha = self.validate_alpha(alpha)
        self.logger = logger

        self.effective_rate = math.floor(self.rate * self.alpha)
        self.in_queue = deque()
        self.processing_deque = deque()

    @staticmethod
    def validate_alpha(alpha: float):
        if alpha < 0 or alpha > 1:
            raise PyChillLimiterInvalidAlpha(f'{alpha=}')
        return alpha

    async def run(self, awaitable, ):
        event = asyncio.Event()
        self.in_queue.appendleft(event)
        logger.debug(f'schedule {awaitable=}')

        # in case, there are no predecessors
        if self.in_queue[-1] is event:
            logger.debug(f'the awaitable is the first item: {awaitable=}')
            event.set()

        await event.wait()
        logger.debug(f'event happened for {awaitable=}')

        # huge while loop to be sure that we do not overflow limit
        while True:
            now = datetime.now()
            # remove expired timestamps
            while (len(self.processing_deque) > 0) and ((now - self.processing_deque[0]) > self.window):
                self.processing_deque.popleft()

            # check if there is room for new requests
            if len(self.processing_deque) >= self.effective_rate:
                # if no room, sleep a little bit
                time_to_sleep = self.window - (now - self.processing_deque[0])
                logger.debug(f'sleep for {time_to_sleep.total_seconds()} seconds before run an {awaitable=}')
                await asyncio.sleep(time_to_sleep.total_seconds())
                continue
            break

        now = datetime.now()
        assert self.in_queue[-1] is event
        self.in_queue.pop()
        self.processing_deque.append(now)

        logger.debug(f"run {awaitable=}")
        try:
            return await awaitable
        except:
            raise
        finally:
            # call successors if present
            if len(self.in_queue):
                self.in_queue[-1].set()

    def decorator(self, func):
        async def wrapper(*args, **kwargs):
            return await self.run(func(*args, **kwargs))

        return wrapper

    async def gather(self, *awaitables, return_exceptions=False):
        return await asyncio.gather(
            *(self.run(i) for i in awaitables),
            return_exceptions=return_exceptions,
        )
