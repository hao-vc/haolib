"""Idempotency middleware."""

from collections.abc import Awaitable, Callable
from datetime import timedelta

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from haolib.exceptions.idempotency import IdempotentRequest
from haolib.utils.typography import to_constant_case

REDIRECT_STATUS_CODES_LOWER_BOUND = 300
REDIRECT_STATUS_CODES_UPPER_BOUND = 399


class IdempotencyKeysStorage:
    """Idempotency keys storage."""

    def __init__(self, redis: Redis, ttl: timedelta) -> None:
        """Initialize the idempotency keys storage.

        Args:
            redis: Redis instance.
            ttl: Time to live for the idempotency key in milliseconds.

        """
        self._redis = redis
        self._ttl = ttl

    async def is_processed(self, idempotency_key: str) -> bool:
        """Check if the idempotency key is processed."""
        response = await self._redis.get(idempotency_key)

        return response is not None

    async def set_processed(self, idempotency_key: str) -> None:
        """Set the idempotency key as processed."""
        await self._redis.set(idempotency_key, idempotency_key, px=self._ttl)


async def idempotency_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
    idempotency_keys_storage: IdempotencyKeysStorage,
) -> Response:
    """Idempotency middleware."""
    idempotency_key = request.headers.get("Idempotency-Key")

    if idempotency_key is None:
        return await call_next(request)

    if await idempotency_keys_storage.is_processed(idempotency_key):
        return JSONResponse(
            status_code=IdempotentRequest.status_code,
            content={
                "detail": IdempotentRequest.detail,
                "error_code": to_constant_case(IdempotentRequest.__name__),
                "additional_info": {},
            },
        )

    response = await call_next(request)

    if not (
        response.status_code >= REDIRECT_STATUS_CODES_LOWER_BOUND
        and response.status_code <= REDIRECT_STATUS_CODES_UPPER_BOUND
    ):
        await idempotency_keys_storage.set_processed(idempotency_key)

    return response
