"""Default exception handlers for FastAPI application."""

import logging

from fastapi import Request
from fastapi.responses import JSONResponse

from haolib.exceptions.base.fastapi import FastAPIBaseException
from haolib.exceptions.schemas.fastapi import FastAPIErrorSchema
from haolib.observability.utils import observe_exception

logger = logging.getLogger(__name__)


async def fastapi_base_exception_handler(
    request: Request,
    exc: FastAPIBaseException,
) -> JSONResponse:
    """Exception handler for FastAPIBaseException.

    Args:
        request: The request.
        exc: The exception.

    Returns:
        JSONResponse: JSON serialized ErrorModel.

    """

    error_schema = FastAPIErrorSchema(
        error_code=exc.current_error_code,
        detail=exc.current_detail,
        additional_info=exc.current_additional_info,
    ).model_dump(mode="json")

    return JSONResponse(
        status_code=exc.current_status_code,
        content=error_schema,
        headers=exc.current_headers,
    )


async def fastapi_unknown_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Exception handler for unknown exceptions.

    Args:
        request: The request.
        exc: The exception.

    Returns:
        JSONResponse: JSON serialized ErrorModel.

    """
    logger.exception("Unknown exception occurred. Details:", exc_info=exc)

    error_schema = FastAPIErrorSchema(
        error_code="UNKNOWN_EXCEPTION",
        detail="Unknown exception occurred",
        additional_info={},
    ).model_dump(mode="json")

    return JSONResponse(status_code=500, content=error_schema)


async def fastapi_unknown_exception_handler_with_observability(request: Request, exc: Exception) -> JSONResponse:
    """Exception handler for unknown exceptions with observability.

    Args:
        request: The request.
        exc: The exception.

    Returns:
        JSONResponse: JSON serialized ErrorModel.

    """
    await observe_exception(exc)
    return await fastapi_unknown_exception_handler(request, exc)
