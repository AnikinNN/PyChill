import asyncio


async def reraise_gather(*coroutines):
    res = await asyncio.gather(*coroutines, return_exceptions=True)
    errors = list(filter(lambda x: isinstance(x, Exception), res))
    if errors:
        raise ExceptionGroup('', errors)
    return res
