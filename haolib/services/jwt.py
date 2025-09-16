"""JWT service for token generation and validation.

This module provides services for JWT (JSON Web Token) handling,
including token generation, validation, and decoding.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from jwt import decode as jwt_decode
from jwt import encode as jwt_encode
from jwt.exceptions import InvalidTokenError
from pydantic import BaseModel, ValidationError

from haolib.configs.jwt import JWTConfig


class JWTService:
    """JWT service for encoding and decoding JSON Web Tokens.

    This service handles the creation and validation of JWTs,
    providing a secure way to transmit information between parties.
    """

    def __init__(self, jwt_config: JWTConfig) -> None:
        """Initialize the JWT service.

        Args:
            jwt_config: The JWT config

        """
        self._secret_key = jwt_config.secret_key
        self._jwt_algorithm = jwt_config.algorithm

    def encode(
        self,
        context_data: BaseModel | None = None,
        expires_in: int | None = None,
        additional_claims: dict[str, Any] | None = None,
    ) -> str:
        """Encode data into a JWT.

        Args:
            context_data: The data to encode into the JWT to the `context` field.
            expires_in: The expiration time in minutes (optional).
            additional_claims: Additional claims to include in the JWT payload.

        Returns:
            The encoded JWT string with additional claims and `context` with given context data.

        """
        payload: dict[str, Any] = {}

        if additional_claims:
            payload.update(additional_claims)

        if expires_in is not None:
            payload["exp"] = datetime.now(UTC) + timedelta(minutes=expires_in)

        if context_data:
            payload["context"] = context_data.model_dump(mode="json")

        return jwt_encode(payload, self._secret_key, algorithm=self._jwt_algorithm)

    def decode[T: BaseModel](
        self,
        token: str,
        context_model: type[T] | None = None,
        required_claims: list[str] | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> dict[str, T | Any]:
        """Decode a JWT and validate required claims.

        Args:
            token: The JWT string to decode.
            context_model: The model to decode the JWT into (optional).
            required_claims: The required claims to validate.
            kwargs: Additional kwargs to pass to the `jwt_decode` function.

        Returns:
            The decoded JWT payload as a dictionary.

        Raises:
            jwt.exceptions.InvalidTokenError: If the token is invalid.
            jwt.exceptions.ExpiredSignatureError: If the token has expired.

        """
        if kwargs is None:
            kwargs = {}

        require = []
        if context_model is not None:
            require.append("context")

        if required_claims is not None:
            require.extend(required_claims)

        payload = jwt_decode(
            token,
            key=self._secret_key,
            algorithms=[self._jwt_algorithm],
            options={
                "require": require,
            },
            **kwargs,
        )

        context = payload["context"]

        try:
            payload["context"] = context_model.model_validate_json(context) if context_model else context
        except KeyError as e:
            raise InvalidTokenError("Missing the context field in the JWT payload") from e
        except ValidationError as e:
            raise InvalidTokenError("Payload context validation error") from e
        else:
            return payload
