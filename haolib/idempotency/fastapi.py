"""Idempotency middleware for FastAPI."""

from collections.abc import Awaitable, Callable

from fastapi import Request, Response
from fastapi.responses import JSONResponse

from haolib.idempotency.storage import AbstractIdempotencyKeysStorage

REDIRECT_STATUS_CODES_LOWER_BOUND = 300
REDIRECT_STATUS_CODES_UPPER_BOUND = 399


async def fastapi_default_idempotency_response_factory(
    request: Request, idempotency_keys_storage: AbstractIdempotencyKeysStorage
) -> Response:
    """Default idempotency response factory.

    Args:
        request: The request.
        idempotency_keys_storage: The idempotency keys storage.

    """
    return JSONResponse(
        status_code=409,
        content={
            "error_code": "IDEMPOTENT_REQUEST",
            "detail": "Idempotent request",
            "additional_info": {},
        },
    )


async def fastapi_idempotency_middleware_handler(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
    idempotency_keys_storage: AbstractIdempotencyKeysStorage,
    idempotent_response_factory: Callable[
        [Request, AbstractIdempotencyKeysStorage], Awaitable[Response]
    ] = fastapi_default_idempotency_response_factory,
) -> Response:
    """FastAPI idempotency middleware handler.

    Args:
        request: The request.
        call_next: The next callable.
        idempotent_response_factory: The factory callable to create the idempotent response.
        idempotency_keys_storage: The idempotency keys storage.

    Returns:
        Response: The response.

    """

    idempotency_key = request.headers.get("Idempotency-Key")

    if idempotency_key is None:
        return await call_next(request)

    if await idempotency_keys_storage.is_processed(idempotency_key):
        return await idempotent_response_factory(request, idempotency_keys_storage)

    response = await call_next(request)

    if not (
        response.status_code >= REDIRECT_STATUS_CODES_LOWER_BOUND
        and response.status_code <= REDIRECT_STATUS_CODES_UPPER_BOUND
    ):
        await idempotency_keys_storage.set_processed(idempotency_key)

    return response
