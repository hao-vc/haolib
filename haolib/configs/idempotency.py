"""Idempotency config."""

from pydantic import Field
from pydantic_settings import BaseSettings

FIVE_MINUTES = 60 * 5 * 1000


class IdempotencyConfig(BaseSettings):
    """Idempotency config.

    This config is used to configure the idempotency middleware.

    Attributes:
        ttl (int): Time to live for the idempotency key in milliseconds. Defaults to 5 minutes.

    """

    ttl: int = Field(default=FIVE_MINUTES, description="Time to live for the idempotency key in milliseconds.")
