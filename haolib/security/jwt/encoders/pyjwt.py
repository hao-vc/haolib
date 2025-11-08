"""PyJWT encoder."""

from datetime import UTC, datetime, timedelta
from typing import Any

from jwt import decode as jwt_decode
from jwt import encode as jwt_encode
from jwt.exceptions import InvalidTokenError
from pydantic import BaseModel, ValidationError

from haolib.security.jwt.encoders.abstract import AbstractJWTEncoder


class PyJWTEncoder(AbstractJWTEncoder):
    """PyJWT encoder for encoding and decoding JSON Web Tokens."""

    def __init__(self, secret_key: str, algorithm: str) -> None:
        """Initialize the PyJWT encoder.

        Args:
            secret_key: The secret key to use for the JWT.
            algorithm: The algorithm to use for the JWT.

        """
        self._secret_key = secret_key
        self._jwt_algorithm = algorithm

    def encode(
        self,
        payload: BaseModel,
        expires_in: timedelta | None = None,
        **kwargs: Any,
    ) -> str:
        """Encode payload into a JWT token.

        Args:
            payload: The payload to encode into the JWT payload.
            expires_in: The expiration time (optional).
            **kwargs: Additional kwargs to pass to the `jwt_encode` function.

        Returns:
            The encoded JWT token string with given payload.

        """
        if expires_in is not None:
            kwargs["exp"] = datetime.now(UTC) + expires_in

        return jwt_encode(
            payload=payload.model_dump(mode="json"),
            key=kwargs.pop("key", self._secret_key),
            algorithm=kwargs.pop("algorithm", self._jwt_algorithm),
            **kwargs,
        )

    def decode[T: BaseModel](
        self,
        token: str,
        payload_class: type[T] | None = None,
        **kwargs: Any,
    ) -> T:
        """Decode a JWT and validate required claims.

        Args:
            token: The JWT string to decode.
            payload_class: The payload class to decode the JWT into (optional).
            **kwargs: Additional kwargs to pass to the `jwt_decode` function.

        Returns:
            The decoded JWT payload as a payload class.

        Raises:
            jwt.exceptions.InvalidTokenError: If the token is invalid.
            jwt.exceptions.ExpiredSignatureError: If the token has expired.

        """

        payload = jwt_decode(
            token,
            key=kwargs.pop("key", self._secret_key),
            algorithms=kwargs.pop("algorithms", [self._jwt_algorithm]),
            **kwargs,
        )

        try:
            return payload_class.model_validate_json(payload) if payload_class else payload
        except ValidationError as e:
            raise InvalidTokenError("Payload validation error") from e
