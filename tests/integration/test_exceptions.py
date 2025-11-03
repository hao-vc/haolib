"""Test exception."""

from typing import TYPE_CHECKING

import pytest
from fastapi import FastAPI, status

from haolib.exceptions.fastapi.base import (
    FastAPIBadRequestException,
    FastAPIBaseException,
    FastAPIConflictException,
    FastAPIForbiddenException,
    FastAPIInternalServerErrorException,
    FastAPIMethodNotAllowedException,
    FastAPINotFoundException,
    FastAPINotImplementedException,
    FastAPIServiceUnavailableException,
    FastAPITooManyRequestsException,
    FastAPIUnauthorizedException,
    FastAPIUnprocessableContentException,
)
from haolib.utils.strings import to_constant_case

if TYPE_CHECKING:
    from fastapi.testclient import TestClient
    from httpx import AsyncClient


class BadRequestExceptionForTest(FastAPIBadRequestException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class UnauthorizedExceptionForTest(FastAPIUnauthorizedException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class ForbiddenExceptionForTest(FastAPIForbiddenException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class NotFoundExceptionForTest(FastAPINotFoundException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class MethodNotAllowedExceptionForTest(FastAPIMethodNotAllowedException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class ConflictExceptionForTest(FastAPIConflictException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class UnprocessableEntityExceptionForTest(FastAPIUnprocessableContentException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class TooManyRequestsExceptionForTest(FastAPITooManyRequestsException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class InternalServerErrorExceptionForTest(FastAPIInternalServerErrorException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class NotImplementedExceptionForTest(FastAPINotImplementedException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class ServiceUnavailableExceptionForTest(FastAPIServiceUnavailableException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exception",
    [
        UnauthorizedExceptionForTest,
        ForbiddenExceptionForTest,
        NotFoundExceptionForTest,
        MethodNotAllowedExceptionForTest,
        ConflictExceptionForTest,
        UnprocessableEntityExceptionForTest,
        TooManyRequestsExceptionForTest,
        InternalServerErrorExceptionForTest,
        NotImplementedExceptionForTest,
        ServiceUnavailableExceptionForTest,
    ],
)
async def test_exception(app: FastAPI, test_client: AsyncClient, exception: type[FastAPIBaseException]) -> None:
    """Test exception."""

    @app.get("/test")
    def handler() -> None:
        """Handler."""
        raise exception

    response = await test_client.get("/test")
    assert response.status_code == exception.status_code
    assert response.json() == {
        "detail": "Test exception",
        "error_code": to_constant_case(exception.__name__),
        "additional_info": exception.additional_info,
    }


class IrregularException(Exception):
    """Irregular exception."""


@pytest.mark.asyncio
async def test_irregular_exceptions(app: FastAPI, test_client_without_raise_server_exceptions: TestClient) -> None:
    """Test irregular exceptions."""

    @app.get("/test")
    def handler() -> None:
        """Handler."""
        raise IrregularException

    response = test_client_without_raise_server_exceptions.get("/test")

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert response.json() == {
        "detail": "Unknown exception occurred",
        "error_code": "UNKNOWN_EXCEPTION",
        "additional_info": {},
    }


@pytest.mark.asyncio
async def test_irregular_exceptions_with_observability(
    app_with_observability: FastAPI, test_client_without_raise_server_exceptions_with_observability: TestClient
) -> None:
    """Test irregular exceptions."""

    @app_with_observability.get("/test")
    def handler() -> None:
        """Handler."""
        raise IrregularException

    response = test_client_without_raise_server_exceptions_with_observability.get("/test")

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert response.json() == {
        "detail": "Unknown exception occurred",
        "error_code": "UNKNOWN_EXCEPTION",
        "additional_info": {},
    }
