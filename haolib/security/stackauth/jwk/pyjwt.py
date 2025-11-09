"""Async PyJWKClient."""

from uuid import UUID

from jwt import InvalidTokenError, decode
from pydantic import BaseModel

from haolib.configs.jwt import JWKConfig
from haolib.security.stackauth.jwk.abstract import AbstractStackAuthJWKEncoder
from haolib.security.utils.pyjwt import AsyncPyJWKClient


class StackAuthJWKTokenPayload(BaseModel):
    """StackAuth JWK token payload."""

    user_id: UUID
    is_anonymous: bool


class StackAuthJWKEncoder(AbstractStackAuthJWKEncoder):
    """StackAuth JWK encoder."""

    def __init__(self, project_id: str, jwk_config: JWKConfig) -> None:
        """Initialize the StackAuth JWK encoder."""
        self._project_id = project_id
        self._pyjwk_client = AsyncPyJWKClient(jwk_config.uri)

    async def decode(self, token: str) -> StackAuthJWKTokenPayload | None:
        """Decode a JWT."""
        try:
            signing_key = await self._pyjwk_client.get_signing_key_from_jwt(token)
            payload = decode(
                token,
                signing_key.key,
                algorithms=["ES256"],
                audience=self._project_id,
            )

            return StackAuthJWKTokenPayload(user_id=UUID(payload["sub"]), is_anonymous=payload.get("role") == "anon")
        except InvalidTokenError:
            return None
        except Exception:
            return None
