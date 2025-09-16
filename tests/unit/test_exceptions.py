"""Tests exceptions."""

from haolib.exceptions.base import AbstractException, BadRequestException, NotFoundException
from haolib.exceptions.handler import to_error_schema


class NotFoundExceptionForTest(NotFoundException):
    """NotFoundException."""

    detail = "A not found exception"
    additional_info = {"test": "test"}


class BadRequestExceptionForTest(BadRequestException):
    """BadRequestException."""

    detail = "A bad request exception"
    additional_info = {"test": "test"}


def test_to_error_schema() -> None:
    """Test to_error_schema."""

    exc_list = [NotFoundExceptionForTest, BadRequestExceptionForTest]

    error_schema = to_error_schema(exc_list)

    assert error_schema.model_fields["error_code"].examples
    assert (
        error_schema.model_fields["error_code"].examples[0]
        == "NOT_FOUND_EXCEPTION_FOR_TEST OR BAD_REQUEST_EXCEPTION_FOR_TEST"
    )

    assert error_schema.model_fields["detail"].examples

    assert error_schema.model_fields["detail"].examples[0] == "A not found exception OR A bad request exception"


class CustomException(AbstractException):
    """Custom exception."""

    detail = "A custom exception {test}"


def test_exception_formatting() -> None:
    """Test exception formatting."""

    exc = CustomException(test="12345", headers={"X-Test": "test"}, additional_info={"anything_else": 123})

    assert exc.detail == "A custom exception 12345"

    assert exc.headers == {"X-Test": "test"}

    assert exc.additional_info == {"anything_else": 123}


def test_exception_representation() -> None:
    """Test exception representation."""

    exc = CustomException(test="12345", headers={"X-Test": "test"}, additional_info={"anything_else": 123})

    assert str(exc) == "<CustomException (code: 500)> A custom exception 12345"
    assert repr(exc) == "<CustomException (code: 500)> A custom exception 12345"
