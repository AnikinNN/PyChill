import asyncio
import uuid
from datetime import timedelta, datetime, timezone
from enum import StrEnum, auto
from functools import wraps
from logging import getLogger, Logger
from typing import Self, Awaitable, Any, Literal, Callable

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

    @property
    def full_config(self):
        return f"{self.base}:{self.config}"


class RedisPyChillConfig(BaseModel):
    names: RedisPyChillConfigNames = Field(default_factory=RedisPyChillConfigNames)
    ack_timeout: timedelta = Field(default=timedelta(seconds=10))
    leader_heartbeat_timeout: timedelta = Field(default=timedelta(seconds=10))
    leader_heartbeat_alpha: float = Field(
        default=0.95,
        description="time to treat current leader dead is `leader_heartbeat_timeout`, so heartbeat must be a little bit earlier",
        gt=0,
        lt=1.0,
    )
    leader_page: int = Field(default=10, ge=1, )


class EventTypes(StrEnum):
    # per limiter
    limiter_configured = auto()
    heartbeat = auto()

    # per run
    run_created = auto()
    invite = auto()
    ack = auto()
    waiting = auto()
    started = auto()
    finished = auto()
    dropped = auto()


class BaseEvent(BaseModel):
    type: EventTypes
    limiter_id: str


class LimiterEvent(BaseEvent):
    type: Literal[
        EventTypes.limiter_configured,
        EventTypes.heartbeat,
    ]
    limiter_id: str


class RunEvent(LimiterEvent):
    type: Literal[
        EventTypes.run_created,
        EventTypes.ack,
        EventTypes.started,
        EventTypes.finished,
        EventTypes.dropped,
    ]
    run_id: str


class InviteEvent(RunEvent):
    type: Literal[EventTypes.invite]
    invited_id: str


class WaitingEvent(RunEvent):
    type: Literal[EventTypes.waiting]
    waiting_for: timedelta


def event_validate(data: dict[bytes, bytes]) -> LimiterEvent:
    data = bytes_dict_decode(data)
    # the order is sufficient, LimiterEvent must be the last
    for klass in (
            WaitingEvent,
            InviteEvent,
            RunEvent,
            LimiterEvent,
    ):
        try:
            return klass.model_validate(data)
        except ValidationError:
            pass
    raise ValidationError(f"data didn't match any model:\n{data=}")


class AutoClearEvent:
    def __init__(self, logger_: Logger = logger):
        self.event = asyncio.Event()
        self.payload: LimiterEvent | RunEvent | InviteEvent | WaitingEvent | None = None
        self.logger = logger_

    def set(self, payload: LimiterEvent):
        if self.payload and payload.type != EventTypes.run_created:
            self.logger.warning(
                f"Payload replacement detected:\n"
                f"Old: {self.payload}\n"
                f"New: {payload}"
            )
        self.payload = payload
        self.event.set()

    async def wait_and_clear(self) -> LimiterEvent:
        """Wait for the event and clear it immediately after. Return payload"""
        await self.event.wait()
        self.event.clear()
        result = self.payload
        self.payload = None
        return result

    async def wait(self) -> LimiterEvent:
        """Wait for the event. Return payload"""
        await self.event.wait()
        return self.payload

    def clear(self):
        """Clear event."""
        self.event.clear()
        self.payload = None


def bytes_dict_decode(data: dict[bytes, bytes]) -> dict[str, str]:
    new = dict()
    for k, v in data.items():
        new[k.decode()] = v.decode()
    return new


class RedisPyChillLimiter(BasePyChillLimiter):
    redis: Redis
    config: RedisPyChillConfig = Field(default_factory=RedisPyChillConfig)
    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    logger: Logger = Field(default=logger)

    _leadership_task: Awaitable = PrivateAttr()
    _leadership_watchdog_task: Awaitable = PrivateAttr()
    _subscriber_task: Awaitable = PrivateAttr()

    _stop_event: asyncio.Event = PrivateAttr()

    def model_post_init(self, context: Any, /) -> None:
        self._stop_event = asyncio.Event()

    @staticmethod
    def log_error(func):
        @wraps(func)
        async def log_error_wrapper(self, *args, **kwargs):
            try:
                return await func(self, *args, **kwargs)
            except Exception as e:
                self.logger.exception(e)
                raise

        return log_error_wrapper

    async def __aenter__(self) -> Self:
        self._subscriber_task = asyncio.create_task(self.subscribe())
        self._leadership_watchdog_task = asyncio.create_task(self.leadership_watchdog_task())
        await self.assert_remote_config()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop_manager()
        # todo cancel all tasks

    @log_error
    async def subscribe(self):
        self.logger.debug(f"subscribe: started")
        cursor = '$'
        while True:
            raw_event = await self.redis.xread(
                streams={self.config.names.full_stream: cursor},
                count=1,
                block=0,
            )
            event = event_validate(raw_event[0][1][0][1])
            cursor = raw_event[0][1][0][0]
            self.logger.debug(f"subscribe: {event=}")

    @staticmethod
    def convert_condition(func):
        async def convert_condition_wrapper(
                self,
                condition: Callable[[LimiterEvent], bool] | EventTypes
        ) -> LimiterEvent:
            if isinstance(condition, EventTypes):
                def a(x):
                    return x.type == condition

                new_condition = a
            else:
                new_condition = condition

            return await func(self, new_condition)

        return convert_condition_wrapper

    @convert_condition
    async def wait_for_event(self, condition: Callable[[LimiterEvent], bool] | EventTypes) -> LimiterEvent:
        f = asyncio.create_task(self.wait_for_event_forward(condition))
        b = asyncio.create_task(self.wait_for_event_backward(condition))

        done, _ = await asyncio.wait([f, b], return_when=asyncio.FIRST_COMPLETED)
        if len(done) != 1:
            return await f

        if b in done:
            b_res = await b
            if b_res is None:
                return await f
            else:
                f.cancel()
                await self.await_cancelled(f)
                return b_res
        elif f in done:
            b.cancel()
            await self.await_cancelled(b)
            return await f
        else:
            raise RuntimeError(f'Impossible state: {done=}')

    @convert_condition
    async def wait_for_event_backward(self, condition: Callable[[LimiterEvent], bool]) -> LimiterEvent | None:
        cursor = '+'
        while True:
            raw_events = await self.redis.xrevrange(
                name=self.config.names.full_stream,
                max=cursor,
                min='-',
                count=self.config.leader_page,
            )
            events = [event_validate(i) for _, i in raw_events]
            if len(events) == 0:
                # stream ended
                return None
            cursor = b'(' + raw_events[0][0]

            for i in events:
                if condition(i):
                    return i

    @convert_condition
    async def wait_for_event_forward(self, condition: Callable[[LimiterEvent], bool]) -> LimiterEvent:
        cursor = '$'
        while True:
            raw_event = await self.redis.xread(
                streams={self.config.names.full_stream: cursor},
                count=1,
                block=0,
            )
            cursor = raw_event[0][1][0][0]
            event = event_validate(raw_event[0][1][0][1])

            if condition(event):
                return event

    @log_error
    async def leadership_watchdog_task(self):
        self.logger.debug(f"leadership_watchdog_task: started")

        await self.try_to_become_a_leader()

        while True:
            try:
                async with asyncio.timeout(self.config.leader_heartbeat_timeout.total_seconds()):
                    await self.wait_for_event_forward(EventTypes.heartbeat)
                    self.logger.debug(f"leadership_watchdog_task: track alive leader")

            except TimeoutError:
                self.logger.debug(f"leadership_watchdog_task: leader died")
                await self.try_to_become_a_leader()

    @log_error
    async def try_to_become_a_leader(self):
        if not await self.redis.get(self.config.names.full_lock):
            self.logger.debug(f"try_to_become_a_leader: no leader found")
            if await self.try_acquire_lock():
                self.logger.debug(f"try_to_become_a_leader: become leader")
                self._leadership_task = asyncio.create_task(self.run_manager())
                # todo mb release lock?

    @log_error
    async def try_acquire_lock(self):
        res = await self.redis.set(
            self.config.names.full_lock,
            self.id,
            nx=True,
            px=self.td_to_tdms(self.config.leader_heartbeat_timeout),
        )
        return bool(res)

    @log_error
    async def assert_remote_config(self):
        raw_remote_config = await self.redis.get(self.config.names.full_config)
        if not raw_remote_config:
            await self.wait_for_event_forward(EventTypes.limiter_configured)
            raw_remote_config = await self.redis.get(self.config.names.full_config)

        remote_config = RedisPyChillConfig.model_validate_json(raw_remote_config)
        if self.config != remote_config:
            raise AssertionError(f'{self.config=}\n!=\n{remote_config=}')

    @log_error
    async def run_manager(self):
        self.logger.debug(f"run_manager: started")
        await self.configure_ts()
        await self.set_config()
        heartbeat = asyncio.create_task(self.heartbeat())
        drop_watchdog = asyncio.create_task(self.drop_watchdog())

        await self._stop_event.wait()
        drop_watchdog.cancel()
        heartbeat.cancel()

        await heartbeat
        await drop_watchdog

        # todo delegate management at finish

        pass

    @log_error
    async def drop_watchdog(self):
        self.logger.debug(f"drop_watchdog: started")

        # find last invite_event or ack_event
        ack_or_invite_event = await self.wait_for_event_backward(
            lambda x: x.type in (EventTypes.invite, EventTypes.ack)
        )

        old_invite_event = (
            ack_or_invite_event
            if (ack_or_invite_event is not None) and (ack_or_invite_event.type == EventTypes.invite)
            else None
        )

        self.logger.debug(f"drop_watchdog: {old_invite_event=}")

        # todo drop if not waiting
        while True:
            if old_invite_event is None:
                invite_event: InviteEvent = await self.wait_for_event_forward(EventTypes.invite)
            else:
                invite_event = old_invite_event
                old_invite_event = None
            try:
                while True:
                    async with asyncio.timeout(self.config.ack_timeout.total_seconds()):
                        ack_event: RunEvent = await self.wait_for_event_forward(EventTypes.ack)
                        if invite_event.invited_id == ack_event.run_id:
                            logger.debug(f"drop_watchdog: expected: {ack_event=}")
                            break
                        else:
                            logger.debug(f"drop_watchdog: unexpected, but ignored: {ack_event=}")
            except TimeoutError:
                # drop
                pop_id = await self.redis.rpop(self.config.names.full_queue, 1)
                assert pop_id == invite_event.invited_id
                # invite next
                invite_id = (await self.redis.lrange(self.config.names.full_queue, -1, -1))[0]
                await self.emit_event(
                    InviteEvent(
                        type=EventTypes.invite,
                        limiter_id=self.id,
                        run_id=self.id,
                        invited_id=invite_id,
                    )
                )

    @log_error
    async def set_config(self):
        await self.redis.set(self.config.names.full_config, self.config.model_dump_json())
        await self.emit_event(
            LimiterEvent(
                type=EventTypes.limiter_configured,
                limiter_id=self.id,
            )
        )
        self.logger.debug(f"set_config: done")

    @log_error
    async def configure_ts(self):
        ts_exists = await self.redis.exists(self.config.names.full_time_series)
        if not ts_exists:
            await self.redis.ts().create(
                self.config.names.full_time_series,
                retention_msecs=self.td_to_tdms(self.window),
                # todo set duplicate_policy
            )

    @log_error
    async def heartbeat(self):
        while True:
            self.logger.debug(f"heartbeat")
            await self.emit_event(
                LimiterEvent(
                    type=EventTypes.heartbeat,
                    limiter_id=self.id,
                )
            )
            await asyncio.sleep(
                self.config.leader_heartbeat_timeout.total_seconds()
                *
                self.config.leader_heartbeat_alpha
            )

    @log_error
    async def stop_manager(self):
        self._stop_event.set()

    async def emit_event(self, event: LimiterEvent):
        return await self.redis.xadd(
            self.config.names.full_stream,
            event.model_dump(mode='json'),
        )

    @staticmethod
    def dt_to_ts(dt: datetime):
        return int(dt.timestamp() * 1000)

    @staticmethod
    def ts_to_dt(ts: int):
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)

    @staticmethod
    def td_to_tdms(td: timedelta):
        return int(td.total_seconds() * 1000)

    @staticmethod
    def tdms_to_td(tdms: int):
        return timedelta(milliseconds=tdms)

    @staticmethod
    def now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    async def await_cancelled(coro):
        try:
            await coro
            raise RuntimeError('Not cancelled')
        except asyncio.CancelledError:
            pass
        except:
            raise

    @log_error
    async def run(self, awaitable, ):
        # generate uuid and put it to the queue
        # convert uuid to str just because it is used only as string in redis and is not used anywhere else as real uuid
        self.logger.debug(f"run: start")

        run_id = uuid.uuid4().hex
        queue_pos = await self.redis.lpush(self.config.names.full_queue, run_id)
        i_am_invited = queue_pos == 1
        self.logger.debug(f"run: queue pos: {queue_pos}")

        # await self.emit_event(
        #     RunEvent(
        #         type=EventTypes.run_created,
        #         limiter_id=self.id,
        #         run_id=run_id,
        #     )
        # )

        # subscribe to the stream
        while not i_am_invited:
            event: InviteEvent = await self.wait_for_event(EventTypes.invite)
            i_am_invited = event.invited_id == run_id

        # emit ack event
        await self.emit_event(
            RunEvent(
                type=EventTypes.ack,
                limiter_id=self.id,
                run_id=run_id,
            )
        )

        last, second = await asyncio.gather(*(
            self.redis.lindex(self.config.names.full_queue, i)
            for i in (-1, -2)
        ))

        assert last.decode() == run_id
        await self.redis.rpop(self.config.names.full_queue, count=1)

        while True:
            # remove expired items from time_series
            now = self.now()
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
                self.logger.debug(f"There is room for new requests, {run_id=}")
                break
            # if no room, sleep a little bit
            oldest_ts = await self.redis.ts().range(self.config.names.full_time_series, '-', '+', 1, )
            oldest_ts = oldest_ts[0][0]
            oldest_ts = self.ts_to_dt(oldest_ts)
            time_to_sleep = self.window - (now - oldest_ts)
            self.logger.debug(f'sleep for {time_to_sleep.total_seconds()} seconds before run an {awaitable=}')
            await self.emit_event(
                WaitingEvent(
                    type=EventTypes.waiting,
                    limiter_id=self.id,
                    run_id=run_id,
                    waiting_for=time_to_sleep,
                )
            )
            await asyncio.sleep(time_to_sleep.total_seconds())

        # set ts to storage
        await self.redis.ts().add(
            key=self.config.names.full_time_series,
            timestamp='*',
            value=1,
        )

        # emit started event
        # await self.emit_event(
        #     RunEvent(
        #         type=EventTypes.started,
        #         limiter_id=self.id,
        #         run_id=run_id,
        #     )
        # )

        if second:
            await self.emit_event(
                InviteEvent(
                    type=EventTypes.invite,
                    limiter_id=self.id,
                    run_id=run_id,
                    invited_id=second,
                )
            )

        return await awaitable
