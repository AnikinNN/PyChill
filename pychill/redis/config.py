from datetime import timedelta

from pydantic import BaseModel, Field


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
