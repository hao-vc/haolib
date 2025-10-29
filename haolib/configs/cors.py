"""CORS config."""

from pydantic import BaseModel, Field


class CORSConfig(BaseModel):
    """CORS config."""

    allow_origins: list[str] = Field(
        description=(
            "The origins to allow in the request. "
            "See https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Access-Control-Allow-Origin "
            "for more information."
        ),
        default_factory=list,
    )
    allow_methods: list[str] = Field(
        description=(
            "The methods to allow in the request. "
            "See https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Access-Control-Allow-Methods "
            "for more information."
        ),
        default_factory=lambda: ["GET"],
    )
    allow_headers: list[str] = Field(
        description=(
            "The headers to allow in the request. "
            "See https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Access-Control-Allow-Headers "
            "for more information."
        ),
        default_factory=list,
    )
    allow_credentials: bool = Field(
        description=(
            "Whether to allow credentials in the request. "
            "See https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Access-Control-Allow-Credentials "
            "for more information."
        ),
        default=False,
    )
    allow_origin_regex: str | None = Field(
        description=(
            "The regex to allow in the request. "
            "See https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Access-Control-Allow-Origin "
            "for more information."
        ),
        default=None,
    )
    expose_headers: list[str] = Field(
        description=(
            "The headers to expose to the client. "
            "See https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Access-Control-Expose-Headers "
            "for more information."
        ),
        default_factory=list,
    )
    max_age: int = Field(
        description=(
            "The maximum age of the preflight request in seconds. "
            "See https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Access-Control-Max-Age "
            "for more information."
        ),
        default=600,
    )
