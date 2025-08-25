from datetime import timedelta
from enum import StrEnum, auto
from typing import Literal, Any

from pydantic import BaseModel, ValidationError

from pychill.redis.utils import bytes_dict_decode


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

    def model_post_init(self, context: Any, /) -> None:
        # exact type matching
        if type(self) is BaseEvent:
            raise NotImplementedError('Not intended to be instantiated')


class LimiterEvent(BaseEvent):
    type: Literal[
        EventTypes.limiter_configured,
        EventTypes.heartbeat,
    ]

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


# class AutoClearEvent:
#     def __init__(self, logger_: Logger = logger):
#         self.event = asyncio.Event()
#         self.payload: LimiterEvent | RunEvent | InviteEvent | WaitingEvent | None = None
#         self.logger = logger_
#
#     def set(self, payload: LimiterEvent):
#         if self.payload and payload.type != EventTypes.run_created:
#             self.logger.warning(
#                 f"Payload replacement detected:\n"
#                 f"Old: {self.payload}\n"
#                 f"New: {payload}"
#             )
#         self.payload = payload
#         self.event.set()
#
#     async def wait_and_clear(self) -> LimiterEvent:
#         """Wait for the event and clear it immediately after. Return payload"""
#         await self.event.wait()
#         self.event.clear()
#         result = self.payload
#         self.payload = None
#         return result
#
#     async def wait(self) -> LimiterEvent:
#         """Wait for the event. Return payload"""
#         await self.event.wait()
#         return self.payload
#
#     def clear(self):
#         """Clear event."""
#         self.event.clear()
#         self.payload = None
