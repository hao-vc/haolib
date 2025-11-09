"""Error schema for API exceptions."""

from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel, Field, create_model

from haolib.exceptions.base.fastapi import FastAPIBaseException


class FastAPIErrorSchema(BaseModel):
    """Error response schema for FastAPIBaseException exceptions."""

    detail: str | None
    error_code: str
    additional_info: dict[str, Any] = Field(
        description="Additional computer-readable information.",
        examples=[{}],
    )

    @classmethod
    def from_exceptions(cls, exceptions: Iterable[type[FastAPIBaseException]]) -> type[FastAPIErrorSchema]:
        """Create error schema from FastAPIAbstractException exceptions."""
        return create_model(
            "ErrorSchemaFor" + "And".join([exc.__name__.replace("FastAPI", "") for exc in exceptions]),
            error_code=(
                str,
                Field(
                    description=" OR ".join([exc.get_class_error_code() for exc in exceptions]),
                    examples=[" OR ".join([exc.get_class_error_code() for exc in exceptions])],
                ),
            ),
            detail=(
                str | None,
                Field(
                    description=" OR ".join([exc.detail for exc in exceptions]),
                    examples=[" OR ".join([exc.detail for exc in exceptions])],
                ),
            ),
            __base__=FastAPIErrorSchema,
        )
