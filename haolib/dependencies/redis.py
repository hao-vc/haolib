"""Redis provider."""

from collections.abc import AsyncGenerator

from dishka import Provider, Scope, provide
from redis.asyncio import ConnectionPool, Redis

from haolib.configs.redis import RedisConfig


class RedisProvider(Provider):
    """Redis provider."""

    @provide(scope=Scope.APP)
    def redis_pool(self, redis_config: RedisConfig) -> ConnectionPool:
        """Get redis pool.

        Args:
            redis_config (RedisConfig): The redis configuration.

        Returns:
            ConnectionPool: The redis pool.

        """
        return ConnectionPool.from_url(str(redis_config.url))

    @provide(scope=Scope.REQUEST)
    async def redis(self, redis_pool: ConnectionPool) -> AsyncGenerator[Redis]:
        """Get redis.

        Args:
            redis_pool (ConnectionPool): The redis pool.

        Returns:
            Redis: The redis instance.

        """
        redis = Redis(connection_pool=redis_pool)
        try:
            yield redis
        finally:
            await redis.aclose()
