"""Abstract base classes for exceptions."""

import string
from abc import ABCMeta
from logging import getLogger
from typing import Any

from fastapi import HTTPException, status

from haolib.exceptions.abstract import AbstractException
from haolib.utils.strings import to_constant_case

logger = getLogger(__name__)


class FastAPIBaseException(AbstractException, HTTPException, metaclass=ABCMeta):
    """FastAPI abstract exception.

    All custom http exceptions must inherit from this class.

    Example:
    ```
        class MyException(FastAPIBaseException):
            status_code = status_codes.HTTP_400_BAD_REQUEST
            detail = "My custom exception"
            headers = {"X-Error": "There goes my error"}

        class MyExceptionWithInit(FastAPIBaseException):
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
        error_code: str | None = None,
        additional_info: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Exception init method.

        Args:
            detail (str): Exception detail.
            status_code (int): HTTP status_codes code.
            headers (dict): Headers to be added to the response.
            error_code (str): Error code to be added to the response.
            additional_info (dict): Additional info to be added to the response.
            **kwargs (Any): Additional kwargs to be added to the exception.

        """
        self.additional_info = additional_info or self.additional_info or {}
        self._initialize_attributes(
            detail=detail,
            status_code=status_code,
            headers=headers,
            additional_info=additional_info,
            error_code=error_code,
        )
        self._format_detail_from_kwargs(kwargs)
        super().__init__(
            status_code=self.current_status_code,
            detail=self.current_detail,
            headers=self.current_headers,
        )

    @classmethod
    def get_class_error_code(cls) -> str:
        """Get error code. It's a constant case of the class name."""
        return to_constant_case(cls.__name__.replace("FastAPI", ""))

    @property
    def error_code(self) -> str:
        """Error code."""
        return self.current_error_code

    def _initialize_attributes(
        self,
        detail: str | None,
        status_code: int | None,
        headers: dict[str, str] | None,
        error_code: str | None = None,
        *,
        additional_info: dict[str, Any] | None,
    ) -> None:
        """Initialize exception attributes with provided or default values."""
        self.current_detail = detail or self.detail
        self.current_headers = (self.headers or {}) | (headers or {})
        self.current_status_code = status_code or self.status_code
        self.current_additional_info = (self.additional_info or {}).copy() | (additional_info or {})
        self.current_error_code = error_code or self.get_class_error_code()

    def _format_detail_from_kwargs(self, kwargs: dict[str, Any]) -> None:
        """Format exception detail message using kwargs if applicable."""
        arguments_to_format = [tup[1] for tup in string.Formatter().parse(self.current_detail) if tup[1]]

        if not arguments_to_format:
            return

        if not kwargs:
            message = (
                f"Detail '{self.current_detail}' contains format specifiers "
                "but no corresponding arguments were passed to the exception constructor"
            )
            raise ValueError(message)

        if not all(arg in kwargs for arg in arguments_to_format):
            message = (
                f"Detail '{self.current_detail}' contains format specifiers "
                "that are not passed to the exception constructor as arguments"
            )
            raise ValueError(message)

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
class FastAPIBadRequestException(FastAPIBaseException):
    """400 Bad Request."""

    status_code = status.HTTP_400_BAD_REQUEST


class FastAPIUnauthorizedException(FastAPIBaseException):
    """401 Unauthorized."""

    status_code = status.HTTP_401_UNAUTHORIZED


class FastAPIForbiddenException(FastAPIBaseException):
    """403 Forbidden."""

    status_code = status.HTTP_403_FORBIDDEN


class FastAPINotFoundException(FastAPIBaseException):
    """404 Not Found."""

    status_code = status.HTTP_404_NOT_FOUND


class FastAPIMethodNotAllowedException(FastAPIBaseException):
    """405 Method Not Allowed."""

    status_code = status.HTTP_405_METHOD_NOT_ALLOWED


class FastAPIConflictException(FastAPIBaseException):
    """409 Conflict."""

    status_code = status.HTTP_409_CONFLICT


class FastAPIGoneException(FastAPIBaseException):
    """410 Gone."""

    status_code = status.HTTP_410_GONE


class FastAPIUnprocessableContentException(FastAPIBaseException):
    """422 Unprocessable Content."""

    status_code = status.HTTP_422_UNPROCESSABLE_CONTENT


class FastAPITooManyRequestsException(FastAPIBaseException):
    """429 Too Many Requests."""

    status_code = status.HTTP_429_TOO_MANY_REQUESTS


class FastAPIInternalServerErrorException(FastAPIBaseException):
    """500 Internal Server Error."""

    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR


class FastAPINotImplementedException(FastAPIBaseException):
    """501 Not Implemented."""

    status_code = status.HTTP_501_NOT_IMPLEMENTED


class FastAPIServiceUnavailableException(FastAPIBaseException):
    """503 Service Unavailable."""

    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
