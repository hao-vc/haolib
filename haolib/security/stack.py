"""StackAuth client."""

from uuid import UUID

from jwt import decode
from jwt.exceptions import InvalidTokenError
from pydantic import BaseModel

from haolib.configs.jwt import JWKConfig
from haolib.security.pyjwt_async import AsyncPyJWKClient


class StackAuthJWKTokenPayload(BaseModel):
    """StackAuth JWK token payload."""

    user_id: UUID
    is_anonymous: bool


class StackAuthJWKService:
    """StackAuth JWK service."""

    def __init__(self, jwk_config: JWKConfig) -> None:
        """Initialize the StackAuth JWK service."""
        self._pyjwk_client = AsyncPyJWKClient(jwk_config.uri)

    async def decode(self, token: str) -> StackAuthJWKTokenPayload | None:
        """Decode a JWT."""
        try:
            signing_key = await self._pyjwk_client.get_signing_key_from_jwt(token)
            payload = decode(
                token,
                signing_key.key,
                algorithms=["ES256"],
                audience="<your-project-id>",
            )

            return StackAuthJWKTokenPayload(user_id=UUID(payload["sub"]), is_anonymous=payload.get("role") == "anon")
        except InvalidTokenError:
            return None
        except Exception:
            return None
