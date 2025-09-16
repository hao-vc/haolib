"""Abstract base classes for exceptions."""

from abc import ABCMeta
from collections.abc import Sequence
from logging import getLogger
from typing import Any, TypedDict
from uuid import UUID

from fastapi import HTTPException, status

logger = getLogger(__name__)


class ExceptionConfigDict(TypedDict, total=False):
    """Exception config dict."""

    detail: str | None
    status_code: int
    headers: dict[str, str] | None
    log_exception: bool
    additional_info: dict[str, Any]
    auto_additional_info_fields: Sequence[str]
    format_detail_from_kwargs: bool


class ApiException(HTTPException):
    """Base exception for API."""


class AbstractException(ApiException, metaclass=ABCMeta):
    """Abstract exception.

    All custom http exceptions must inherit from this class.

    Example:
    ```
        class MyException(AbstractException):
            status_code = status_codes.HTTP_400_BAD_REQUEST
            detail = "My custom exception"
            headers = {"X-Error": "There goes my error"}

        class MyExceptionWithInit(AbstractException):
            def __init__(
                self,
                detail: str = "My custom exception",
                status_code: int = status_codes.HTTP_400_BAD_REQUEST,
                headers: dict[str, str] = {"X-Error": "There goes my error"},
            ) -> None:
                # In __init__ we can do more complex logic, like setting status_code
                # based on some condition.
                super().__init__lib.(detail, status_code, headers)
    ```

    """

    detail: str
    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR
    headers: dict[str, str] | None = None
    log_exception: bool = True
    additional_info: dict[str, Any] = {}
    format_detail_from_kwargs: bool = True

    def __init__(
        self,
        detail: str | None = None,
        status_code: int | None = None,
        *,
        headers: dict[str, str] | None = None,
        additional_info: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Exception init method.

        Args:
            detail (str): Exception detail.
            status_code (int): HTTP status_codes code.
            headers (dict): Headers to be added to the response.
            additional_info (dict): Additional info to be added to the response.
            **kwargs (Any): Additional kwargs to be added to the exception.

        """
        self.additional_info = additional_info or self.additional_info or {}
        self._initialize_attributes(
            detail=detail,
            status_code=status_code,
            headers=headers,
            additional_info=additional_info,
        )
        self._format_detail_from_kwargs(kwargs)
        super().__init__(
            status_code=self.current_status_code,
            detail=self.current_detail,
            headers=self.current_headers,
        )

    def _initialize_attributes(
        self,
        detail: str | None,
        status_code: int | None,
        headers: dict[str, str] | None,
        *,
        additional_info: dict[str, Any] | None,
    ) -> None:
        """Initialize exception attributes with provided or default values."""
        self.current_detail = detail or self.detail
        self.current_headers = (self.headers or {}) | (headers or {})
        self.current_status_code = status_code or self.status_code
        self.current_additional_info = (self.additional_info or {}).copy() | (additional_info or {})

    def _format_detail_from_kwargs(self, kwargs: dict[str, Any]) -> None:
        """Format exception detail message using kwargs if applicable."""
        if not (kwargs and self.current_detail is not None and self.format_detail_from_kwargs):
            return

        self.current_detail = self.current_detail.format_map(kwargs)

    def __repr__(self) -> str:
        """Str repr."""
        detail = self.current_detail or "no detail"
        return f"<{self.__class__.__name__} (code: {self.current_status_code})> {detail}"

    def __str__(self) -> str:
        """Str repr."""
        return self.__repr__()


# Define base exceptions for specific HTTP status_codes codes.
# Usage: class MyDomainException(DomainException, NotFoundException): pass
# Order is important here, please see the comment in AbstractException.__init__.
class BadRequestException(AbstractException):
    """400 Bad Request."""

    status_code = status.HTTP_400_BAD_REQUEST


class UnauthorizedException(AbstractException):
    """401 Unauthorized."""

    status_code = status.HTTP_401_UNAUTHORIZED


class ForbiddenException(AbstractException):
    """403 Forbidden."""

    status_code = status.HTTP_403_FORBIDDEN


class NotFoundException(AbstractException):
    """404 Not Found."""

    status_code = status.HTTP_404_NOT_FOUND


class MethodNotAllowedException(AbstractException):
    """405 Method Not Allowed."""

    status_code = status.HTTP_405_METHOD_NOT_ALLOWED


class ConflictException(AbstractException):
    """409 Conflict."""

    status_code = status.HTTP_409_CONFLICT


class GoneException(AbstractException):
    """410 Gone."""

    status_code = status.HTTP_410_GONE


class UnprocessableEntityException(AbstractException):
    """422 Unprocessable Entity."""

    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY


class TooManyRequestsException(AbstractException):
    """429 Too Many Requests."""

    status_code = status.HTTP_429_TOO_MANY_REQUESTS


class InternalServerErrorException(AbstractException):
    """500 Internal Server Error."""

    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR


class NotImplementedException(AbstractException):
    """501 Not Implemented."""

    status_code = status.HTTP_501_NOT_IMPLEMENTED


class ServiceUnavailableException(AbstractException):
    """503 Service Unavailable."""

    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
