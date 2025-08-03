import asyncio
import traceback


async def reraise_gather(*coroutines):
    async def wrapped(coro):
        try:
            return await coro
        except:
            traceback.print_exc()
            raise
    res = await asyncio.gather(*(wrapped(i) for i in coroutines), return_exceptions=True)
    errors = list(filter(lambda x: isinstance(x, Exception), res))
    if errors:
        raise ExceptionGroup('', errors)
    return res
