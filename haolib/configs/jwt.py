"""JWT config."""

from pydantic_settings import BaseSettings


class JWTConfig(BaseSettings):
    """JWT config."""

    secret_key: str
    algorithm: str
    expires_in: int | None = None
