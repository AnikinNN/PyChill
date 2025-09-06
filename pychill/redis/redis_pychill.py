import asyncio
import uuid
from logging import getLogger, Logger
from typing import Any, Awaitable, Self

from pydantic import Field, PrivateAttr
from redis.asyncio import Redis
from redis.commands.timeseries import TSInfo

from pychill.base import BasePyChillLimiter
from pychill.redis.config import RedisPyChillConfig
from pychill.redis.events import BaseEvent, event_validate, EventTypes, InviteEvent, LimiterEvent, RunEvent, \
    WaitingEvent
from pychill.redis.stream_copy import RSCQueue
from pychill.redis.utils import dt_to_ts, get_now, log_error, td_to_tdms, ts_to_dt

logger = getLogger(__name__)


class RedisPyChillLimiter(BasePyChillLimiter):
    redis: Redis
    config: RedisPyChillConfig = Field(default_factory=RedisPyChillConfig)
    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    logger: Logger = Field(default=logger)

    _leadership_task: Awaitable = PrivateAttr()
    _leadership_watchdog_task: Awaitable = PrivateAttr()
    _subscriber_task: Awaitable = PrivateAttr()

    _stop_event: asyncio.Event = PrivateAttr()
    _stream_copy: RSCQueue = PrivateAttr()

    def model_post_init(self, context: Any, /) -> None:
        self._stop_event = asyncio.Event()
        self._stream_copy = RSCQueue()

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
            self._stream_copy.append(event)
            self.logger.debug(f"subscribe: {event=}")
            # todo add new event type and clean stream copy on stream cut

    @log_error
    async def leadership_watchdog_task(self):
        self.logger.debug(f"leadership_watchdog_task: started")

        await self.try_to_become_a_leader()

        while True:
            try:
                async with asyncio.timeout(self.config.leader_heartbeat_timeout.total_seconds()):
                    await self._stream_copy.find_forward(EventTypes.heartbeat)
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
            px=td_to_tdms(self.config.leader_heartbeat_timeout),
        )
        return bool(res)

    @log_error
    async def assert_remote_config(self):
        raw_remote_config = await self.redis.get(self.config.names.full_config)
        if not raw_remote_config:
            await self._stream_copy.find(EventTypes.limiter_configured)
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

        cursor = self._stream_copy.last_node

        # find last invite_event or ack_event
        found = await self._stream_copy.find_backward(
            lambda x: x.type in (EventTypes.invite, EventTypes.ack),
            cursor,
        )

        if found is not None:
            ack_or_invite_event, _ = found
        else:
            ack_or_invite_event = None

        old_invite_event = (
            ack_or_invite_event
            if (ack_or_invite_event is not None) and (ack_or_invite_event.type == EventTypes.invite)
            else None
        )

        self.logger.debug(f"drop_watchdog: {old_invite_event=}")

        # todo drop if not waiting
        while True:
            if old_invite_event is None:
                invite_event: InviteEvent
                invite_event, cursor = await self._stream_copy.find_forward(EventTypes.invite, cursor)
            else:
                invite_event = old_invite_event
                old_invite_event = None
            try:
                while True:
                    async with asyncio.timeout(self.config.ack_timeout.total_seconds()):
                        ack_event: RunEvent
                        ack_event, cursor = await self._stream_copy.find_forward(EventTypes.ack, cursor)
                        if invite_event.invited_id == ack_event.run_id:
                            self.logger.debug(f"drop_watchdog: expected: {ack_event=}")
                            break
                        else:
                            self.logger.debug(f"drop_watchdog: unexpected, but ignored: {ack_event=}")
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
                retention_msecs=td_to_tdms(self.window),
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

            # todo check that you do not steal lock
            await self.redis.set(
                self.config.names.full_lock,
                self.id,
                nx=True,
                px=td_to_tdms(self.config.leader_heartbeat_timeout),
                get=True,
            )

            await asyncio.sleep(
                self.config.leader_heartbeat_timeout.total_seconds()
                *
                self.config.leader_heartbeat_alpha
            )

    @log_error
    async def stop_manager(self):
        self._stop_event.set()

    async def emit_event(self, event: BaseEvent):
        return await self.redis.xadd(
            self.config.names.full_stream,
            event.model_dump(mode='json'),
        )

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
            event, _ = await self._stream_copy.find(EventTypes.invite)
            event: InviteEvent
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
            now = get_now()
            deleted = await self.redis.ts().delete(
                key=self.config.names.full_time_series,
                from_time=0,
                to_time=dt_to_ts(now - self.window),
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
            oldest_ts = ts_to_dt(oldest_ts)
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
