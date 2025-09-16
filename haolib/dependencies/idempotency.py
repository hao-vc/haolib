"""Idempotency dependencies."""

from dishka import Provider, Scope, provide
from redis.asyncio import Redis

from haolib.configs.idempotency import IdempotencyConfig
from haolib.middlewares.idempotency import IdempotencyKeysStorage


class IdempotencyProvider(Provider):
    """Idempotency provider."""

    @provide(scope=Scope.REQUEST)
    async def idempotency_keys_storage(
        self, redis: Redis, idempotency_config: IdempotencyConfig
    ) -> IdempotencyKeysStorage:
        """Get idempotency keys storage.

        Args:
            redis: Redis instance.
            idempotency_config: Idempotency config.

        Returns:
            IdempotencyKeysStorage: The idempotency keys storage.

        """
        return IdempotencyKeysStorage(redis, idempotency_config.ttl)
