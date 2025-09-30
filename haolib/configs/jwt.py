"""JWT config."""

from datetime import timedelta

from pydantic_settings import BaseSettings


class JWTConfig(BaseSettings):
    """JWT config."""

    secret_key: str
    algorithm: str
    expires_in: timedelta | None = None


class JWKConfig(BaseSettings):
    """JWK config."""

    uri: str
