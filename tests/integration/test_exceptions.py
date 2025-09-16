"""Test exception."""

import pytest
from fastapi import FastAPI, status
from fastapi.testclient import TestClient
from httpx import AsyncClient

from haolib.exceptions.base import (
    AbstractException,
    BadRequestException,
    ConflictException,
    ForbiddenException,
    InternalServerErrorException,
    MethodNotAllowedException,
    NotFoundException,
    NotImplementedException,
    ServiceUnavailableException,
    TooManyRequestsException,
    UnauthorizedException,
    UnprocessableContentException,
)
from haolib.utils.typography import to_constant_case


class BadRequestExceptionForTest(BadRequestException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class UnauthorizedExceptionForTest(UnauthorizedException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class ForbiddenExceptionForTest(ForbiddenException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class NotFoundExceptionForTest(NotFoundException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class MethodNotAllowedExceptionForTest(MethodNotAllowedException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class ConflictExceptionForTest(ConflictException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class UnprocessableEntityExceptionForTest(UnprocessableContentException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class TooManyRequestsExceptionForTest(TooManyRequestsException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class InternalServerErrorExceptionForTest(InternalServerErrorException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class NotImplementedExceptionForTest(NotImplementedException):
    """Test exception."""

    detail = "Test exception"
    additional_info = {"test": "test"}


class ServiceUnavailableExceptionForTest(ServiceUnavailableException):
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
async def test_exception(app: FastAPI, test_client: AsyncClient, exception: type[AbstractException]) -> None:
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


class InternalException(AbstractException):
    """Internal exception."""

    is_public = False


@pytest.mark.asyncio
async def test_irregular_exceptions_public_exception(
    app: FastAPI, test_client_without_raise_server_exceptions: TestClient
) -> None:
    """Test irregular exceptions public exception."""

    @app.get("/test")
    def handler() -> None:
        """Handler."""
        raise InternalException

    response = test_client_without_raise_server_exceptions.get("/test")

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert response.json() == {
        "detail": "Unknown exception occurred",
        "error_code": "UNKNOWN_EXCEPTION",
        "additional_info": {},
    }
