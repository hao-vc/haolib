"""StackAuth JWK service."""

from typing import Protocol
from uuid import UUID


class StackAuthJWKTokenPayload(Protocol):
    """StackAuth JWK token payload."""

    user_id: UUID
    is_anonymous: bool


class AbstractStackAuthJWKEncoder(Protocol):
    """Abstract StackAuth JWK encoder."""

    async def decode(self, token: str) -> StackAuthJWKTokenPayload | None:
        """Decode a JWT."""
