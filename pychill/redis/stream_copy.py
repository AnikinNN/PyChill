import asyncio
from typing import Any, Callable

from pychill.redis.events import BaseEvent, EventTypes
from pychill.redis.utils import await_cancelled


class NoPayload:
    pass


class NoPayloadError(Exception):
    pass


class PayloadOverrideError(Exception):
    pass


class EventWithPayload:
    def __init__(self):
        self.event = asyncio.Event()
        self.payload: Any = NoPayload()

    def set(self, payload):
        if not isinstance(self.payload, NoPayload):
            raise PayloadOverrideError(f'{payload}')
        self.payload = payload
        self.event.set()

    async def wait(self):
        await self.event.wait()
        if isinstance(self.payload, NoPayload):
            raise NoPayloadError()
        return self.payload


class RSCQNode:
    def __init__(self):
        # `payload` stores event from redis
        self.payload: NoPayload | BaseEvent = NoPayload()
        # in `event_with_payload` we store next node as payload
        self.event_with_payload = EventWithPayload()
        self.prev_node = None

    async def next(self):
        return await self.event_with_payload.wait()


class RSCQueue:
    def __init__(self):
        self.last_node = RSCQNode()

    @staticmethod
    def convert_condition(func):
        async def convert_condition_wrapper(
                self,
                condition: Callable[[BaseEvent], bool] | EventTypes,
                node: RSCQNode | None = None,
        ) -> BaseEvent:
            if isinstance(condition, EventTypes):
                def a(x):
                    return x.type == condition

                new_condition = a
            else:
                new_condition = condition

            if node is None:
                node = self.last_node

            return await func(self, new_condition, node)

        return convert_condition_wrapper

    def append(self, event: BaseEvent):
        node = RSCQNode()
        node.payload = event
        node.prev_node = self.last_node
        self.last_node.event_with_payload.set(node)
        self.last_node = node

    def get_last_node(self):
        return self.last_node

    @convert_condition
    async def find_forward(self, condition: Callable[[BaseEvent], bool], node: RSCQNode) -> tuple[BaseEvent, RSCQNode]:
        while True:
            node = await node.event_with_payload.wait()
            if condition(node.payload):
                return node.payload, node

    @convert_condition
    async def find_backward(self, condition: Callable[[BaseEvent], bool], node: RSCQNode) -> tuple[
                                                                                                 BaseEvent, RSCQNode] | None:
        while True:
            if isinstance(node.payload, NoPayload):
                return None
            if condition(node.payload):
                return node.payload, node
            node = node.prev_node
            await asyncio.sleep(0)

    async def find(self, condition: Callable[[BaseEvent], bool] | EventTypes) -> tuple[BaseEvent, RSCQNode]:
        last_node = self.last_node
        f = asyncio.create_task(self.find_forward(condition, last_node))
        b = asyncio.create_task(self.find_backward(condition, last_node))

        done, _ = await asyncio.wait([f, b], return_when=asyncio.FIRST_COMPLETED)
        if len(done) != 1:
            return await f

        if b in done:
            b_res = await b
            if b_res is None:
                return await f
            else:
                f.cancel()
                await await_cancelled(f)
                return b_res
        elif f in done:
            b.cancel()
            await await_cancelled(b)
            return await f
        else:
            raise RuntimeError(f'Impossible state: {done=}')
