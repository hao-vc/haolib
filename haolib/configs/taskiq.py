"""Taskiq config."""

from __future__ import annotations

from datetime import timedelta

from pydantic import Field
from pydantic_settings import BaseSettings
from taskiq.acks import AcknowledgeType


class TaskiqWorkerConfig(BaseSettings):
    """Taskiq worker config.

    This config is used to configure the taskiq worker.

    Attributes:
        ack_time (AcknowledgeType): The time to acknowledge the message. Defaults to WHEN_SAVED.

    """

    ack_time: AcknowledgeType = Field(default=AcknowledgeType.WHEN_SAVED)


class TaskiqSchedulerConfig(BaseSettings):
    """Taskiq scheduler config.

    This config is used to configure the taskiq scheduler.

    Attributes:
        schedule_interval (timedelta): The interval of the schedule. Defaults to 1 minute.

    """

    schedule_interval: timedelta = Field(default=timedelta(minutes=1))


class TaskiqConfig(BaseSettings):
    """Taskiq config.

    This config is used to configure the taskiq.

    Attributes:
        worker (TaskiqWorkerConfig): The worker config.
        scheduler (TaskiqSchedulerConfig): The scheduler config.

    """

    worker: TaskiqWorkerConfig = Field(default_factory=TaskiqWorkerConfig)
    scheduler: TaskiqSchedulerConfig = Field(default_factory=TaskiqSchedulerConfig)
