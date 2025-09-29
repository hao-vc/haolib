"""Idempotency config."""

from datetime import timedelta

from pydantic import Field
from pydantic_settings import BaseSettings


class IdempotencyConfig(BaseSettings):
    """Idempotency config.

    This config is used to configure the idempotency middleware.

    Attributes:
        ttl (int): Time to live for the idempotency key in milliseconds. Defaults to 5 minutes.

    """

    ttl: timedelta = Field(default=timedelta(minutes=5), description="Time to live for the idempotency key.")
