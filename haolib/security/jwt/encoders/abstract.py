"""JWT module for abstract interfaces."""

from datetime import timedelta
from typing import Any, Protocol


class AbstractJWTEncoder(Protocol):
    """Abstract JWT encoder."""

    def encode(
        self,
        payload: Any,
        expires_in: timedelta | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> str:
        """Encode payload into a JWT token.

        Args:
            payload: The payload to encode into the JWT payload.
            expires_in: The expiration time (optional).
            *args: Additional args.
            **kwargs: Additional kwargs.

        Returns:
            The encoded JWT token string.

        """
        ...

    def decode[T: Any](
        self,
        token: str,
        payload_class: type[T] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Decode a JWT token into a payload class.

        Args:
            token: The JWT token to decode.
            payload_class: The payload class to decode the JWT payload into (optional).
            *args: Additional args.
            **kwargs: Additional kwargs.

        Returns:
            The decoded JWT payload as a payload class.

        """
        ...
