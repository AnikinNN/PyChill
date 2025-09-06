import asyncio
from datetime import datetime, timedelta, timezone
from functools import wraps


async def await_cancelled(coro):
    try:
        await coro
        raise RuntimeError('Not cancelled')
    except asyncio.CancelledError:
        pass
    except:
        raise


def get_now() -> datetime:
    return datetime.now(timezone.utc)


def tdms_to_td(tdms: int):
    return timedelta(milliseconds=tdms)


def td_to_tdms(td: timedelta):
    return int(td.total_seconds() * 1000)


def ts_to_dt(ts: int):
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)


def dt_to_ts(dt: datetime):
    return int(dt.timestamp() * 1000)


def log_error(func):
    @wraps(func)
    async def log_error_wrapper(self, *args, **kwargs):
        try:
            return await func(self, *args, **kwargs)
        except Exception as e:
            self.logger.exception(e)
            raise

    return log_error_wrapper


def bytes_dict_decode(data: dict[bytes, bytes]) -> dict[str, str]:
    new = dict()
    for k, v in data.items():
        new[k.decode()] = v.decode()
    return new
