"""Redis idempotency keys storage."""

from datetime import timedelta

from redis.asyncio import Redis

from haolib.idempotency.storage.abstract import AbstractIdempotencyKeysStorage


class RedisIdempotencyKeysStorage(AbstractIdempotencyKeysStorage):
    """Redis idempotency keys storage."""

    def __init__(self, redis: Redis, ttl: timedelta) -> None:
        """Initialize the redis idempotency keys storage.

        Args:
            redis: Redis instance.
            ttl: Time to live for the idempotency key.

        """
        self._redis = redis
        self._ttl = ttl

    async def is_processed(self, idempotency_key: str) -> bool:
        """Check if the idempotency key is processed.

        Args:
            idempotency_key: The idempotency key.

        Returns:
            bool: True if the idempotency key is processed, False otherwise.

        """
        response = await self._redis.get(idempotency_key)

        return response is not None

    async def set_processed(self, idempotency_key: str) -> None:
        """Set the idempotency key as processed.

        Args:
            idempotency_key: The idempotency key.

        """
        await self._redis.set(idempotency_key, idempotency_key, px=self._ttl)
