import asyncio
import uuid
from datetime import timedelta, datetime, timezone
from enum import StrEnum, auto
from logging import getLogger, Logger
from typing import Self, Any

from pydantic import Field, BaseModel, ValidationError, PrivateAttr
from redis.asyncio import Redis
from redis.commands.timeseries import TSInfo

from pychill.base import BasePyChillLimiter

logger = getLogger(__name__)


class RedisPyChillConfigNames(BaseModel):
    base: str = Field(default="pychill", description="Name of limiter. Must be unique for every Redis limited")
    lock: str = Field(default="lock", description="Key that is used for lock")
    stream: str = Field(default="stream", description="Key that is used for stream")
    queue: str = Field(default="queue", description="Key that is used for queue")
    time_series: str = Field(default="time_series", description="Key that is used for time series")
    config: str = Field(default="config", description="Key that is used for config hash")

    @property
    def full_lock(self):
        return f"{self.base}:{self.lock}"

    @property
    def full_stream(self):
        return f"{self.base}:{self.stream}"

    @property
    def full_queue(self):
        return f"{self.base}:{self.queue}"

    @property
    def full_time_series(self):
        return f"{self.base}:{self.time_series}"


class RedisPyChillConfig(BaseModel):
    names: RedisPyChillConfigNames = Field(default_factory=RedisPyChillConfigNames)
    ack_timeout: timedelta = Field(default=timedelta(seconds=10))


class RedisPyChillLimiterEventTypes(StrEnum):
    created = auto()
    ack = auto()
    waiting = auto()
    started = auto()
    finished = auto()
    dropped = auto()


class RedisPyChillLimiterEventBase(BaseModel):
    type: RedisPyChillLimiterEventTypes
    run_id: str


class RedisPyChillLimiterWaitingEvent(RedisPyChillLimiterEventBase):
    waiting_for: timedelta


class RedisPyChillLimiterNextEvent(RedisPyChillLimiterEventBase):
    next_run_id: str


def event_validate(data):
    for klass in (
            RedisPyChillLimiterWaitingEvent,
            RedisPyChillLimiterNextEvent,
            RedisPyChillLimiterEventBase,

    ):
        try:
            return klass.model_validate(data)
        except ValidationError:
            pass
    raise ValidationError("didn't match any model")


class RedisPyChillLimiter(BasePyChillLimiter):
    logger: Logger = Field(default=logger)
    redis: Redis = Field(description="async Redis instance, must be initialized outside limiter")
    config: RedisPyChillConfig = Field(default_factory=RedisPyChillConfig)
    _run_next_manager: Any = PrivateAttr()

    async def __aenter__(self) -> Self:
        # todo sync settings create all objs
        await self.redis.ts().create(self.config.names.full_time_series)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def run(self, awaitable):
        # generate uuid and put it to the queue
        # convert uuid to str just because it is used only as string in redis and is not used anywhere else as real uuid
        run_id = uuid.uuid4().hex
        queue_pos = await self.redis.lpush(self.config.names.full_queue, run_id)
        i_am_next = queue_pos == 1
        self.logger.debug(f"Redis queue pos: {queue_pos}")

        await self.emit_event(RedisPyChillLimiterEventTypes.created, run_id)

        # subscribe to the stream
        while not i_am_next:
            event = await self.redis.xread({self.config.names.full_stream: '$'}, block=0)
            event = event_validate(self.bytes_dict_decode(event[0][1][0][1]))
            i_am_next = event.next_id == run_id

        # emit ack event
        await self.emit_event(RedisPyChillLimiterEventTypes.ack, run_id)

        last, second = await asyncio.gather(*(
            self.redis.lindex(self.config.names.full_queue, i)
            for i in (-1, -2)
        ))

        assert last.decode() == run_id
        await self.redis.rpop(self.config.names.full_queue, count=1)

        while True:
            # remove expired items from time_series
            now = datetime.now(tz=timezone.utc)
            deleted = await self.redis.ts().delete(
                key=self.config.names.full_time_series,
                from_time=0,
                to_time=self.dt_to_ts(now - self.window),
            )
            self.logger.debug(f"deleted from ts: {deleted}")

            series_info: TSInfo = await self.redis.ts().info(self.config.names.full_time_series)
            self.logger.debug(f"{series_info.total_samples=}")
            # check if there is room for new requests
            if series_info.total_samples < self.effective_rate:
                self.logger.debug(f"No wait anymore, go-go-go, {run_id=}")
                break
            # if no room, sleep a little bit
            oldest_ts = await self.redis.ts().range(self.config.names.full_time_series, '-', '+', 1, )
            oldest_ts = oldest_ts[0][0]
            oldest_ts = self.ts_to_dt(oldest_ts)
            time_to_sleep = self.window - (now - oldest_ts)
            self.logger.debug(f'sleep for {time_to_sleep.total_seconds()} seconds before run an {awaitable=}')
            await self.emit_event(RedisPyChillLimiterEventTypes.waiting, run_id, waiting_for=time_to_sleep)
            await asyncio.sleep(time_to_sleep.total_seconds())

        # set ts to storage
        await self.redis.ts().add(
            key=self.config.names.full_time_series,
            timestamp='*',
            value=1,
        )

        # emit started event
        await self.emit_event(RedisPyChillLimiterEventTypes.started, run_id, next_id=second)

        return await awaitable

    @staticmethod
    def dt_to_ts(dt: datetime):
        return int(dt.timestamp() * 1000)

    @staticmethod
    def ts_to_dt(ts: int):
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)

    async def emit_event(
            self,
            event_type: RedisPyChillLimiterEventTypes,
            run_id: str,
            next_id: str | None = None,
            waiting_for: timedelta | None = None,
    ):
        event = RedisPyChillLimiterEvent(
            type=event_type,
            run_id=run_id,
            next_id=next_id,
            waiting_for=waiting_for,
        )
        self.logger.debug(event)
        await self.redis.xadd(
            self.config.names.full_stream,
            fields=event.model_dump(exclude_none=True, mode='json'),
        )

    @staticmethod
    def bytes_dict_decode(data: dict[bytes, bytes]) -> dict[str, str]:
        new = dict()
        for k, v in data.items():
            new[k.decode()] = v.decode()
        return new
