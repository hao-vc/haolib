"""Tests exceptions."""

import pytest

from haolib.exceptions.base.fastapi import (
    FastAPIBadRequestException,
    FastAPIBaseException,
    FastAPINotFoundException,
)
from haolib.exceptions.schemas.fastapi import FastAPIErrorSchema


class NotFoundExceptionForTest(FastAPINotFoundException):
    """NotFoundException."""

    detail = "A not found exception"
    additional_info = {"test": "test"}


class BadRequestExceptionForTest(FastAPIBadRequestException):
    """BadRequestException."""

    detail = "A bad request exception"
    additional_info = {"test": "test"}


def test_to_error_schema() -> None:
    """Test to_error_schema."""

    exc_list = [NotFoundExceptionForTest, BadRequestExceptionForTest]

    error_schema = FastAPIErrorSchema.from_exceptions(exc_list)

    assert error_schema.model_fields["error_code"].examples
    assert (
        error_schema.model_fields["error_code"].examples[0]
        == "NOT_FOUND_EXCEPTION_FOR_TEST OR BAD_REQUEST_EXCEPTION_FOR_TEST"
    )

    assert error_schema.model_fields["detail"].examples

    assert error_schema.model_fields["detail"].examples[0] == "A not found exception OR A bad request exception"


class CustomException(FastAPIBaseException):
    """Custom exception."""

    detail = "A custom exception {test}"


class CustomExceptionWithoutFormatSpecifiers(FastAPIBaseException):
    """Custom exception without error code."""

    detail = "A custom exception"


def test_exception_formatting() -> None:
    """Test exception formatting."""

    exc = CustomException(test="12345", headers={"X-Test": "test"}, additional_info={"anything_else": 123})

    assert exc.detail == "A custom exception 12345"

    assert exc.headers == {"X-Test": "test"}

    assert exc.additional_info == {"anything_else": 123}

    exc_without_format_specifiers = CustomExceptionWithoutFormatSpecifiers(
        headers={"X-Test": "test"}, additional_info={"anything_else": 123}
    )

    assert exc_without_format_specifiers.detail == "A custom exception"


def test_exception_representation() -> None:
    """Test exception representation."""

    exc = CustomException(test="12345", headers={"X-Test": "test"}, additional_info={"anything_else": 123})

    assert str(exc) == "<CustomException (code: 500)> A custom exception 12345"
    assert repr(exc) == "<CustomException (code: 500)> A custom exception 12345"


def test_exception_error_code() -> None:
    """Test exception error code."""

    exc = CustomException(test="12345", headers={"X-Test": "test"}, additional_info={"anything_else": 123})

    assert exc.error_code == "CUSTOM_EXCEPTION"

    assert exc.get_class_error_code() == "CUSTOM_EXCEPTION"


class CustomExceptionWithFewFormatSpecifiers(FastAPIBaseException):
    """Custom exception with few format specifiers."""

    detail = "A custom exception {test} {test2}"


def test_exception_format_detail_from_kwargs_raises_error() -> None:
    """Test exception format detail from kwargs."""

    with pytest.raises(ValueError):
        CustomException(headers={"X-Test": "test"}, additional_info={"anything_else": 123})

    with pytest.raises(ValueError):
        CustomException(not_test="12345", headers={"X-Test": "test"}, additional_info={"anything_else": 123})

    with pytest.raises(ValueError):
        CustomExceptionWithFewFormatSpecifiers(
            test2="12345", headers={"X-Test": "test"}, additional_info={"anything_else": 123}
        )

    assert (
        CustomExceptionWithFewFormatSpecifiers(
            test="12345", test2="123456", headers={"X-Test": "test"}, additional_info={"anything_else": 123}
        ).detail
        == "A custom exception 12345 123456"
    )
