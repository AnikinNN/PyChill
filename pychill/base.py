import asyncio
import math
from abc import ABC, abstractmethod
from datetime import timedelta
from logging import getLogger, Logger

from pydantic import BaseModel, ConfigDict, Field

logger = getLogger(__name__)


class BasePyChillLimiter(BaseModel, ABC):
    rate: int
    window: timedelta = Field(default=timedelta(days=1))
    alpha: float = Field(default=0.95, gt=0, le=1)
    logger: Logger

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def effective_rate(self) -> int:
        return math.floor(self.rate * self.alpha)

    def decorator(self, func):
        async def wrapper(*args, **kwargs):
            return await self.run(func(*args, **kwargs))

        return wrapper

    async def gather(self, *awaitables, return_exceptions=False):
        return await asyncio.gather(
            *(self.run(i) for i in awaitables),
            return_exceptions=return_exceptions,
        )

    @abstractmethod
    async def run(self, awaitable, ):
        pass
