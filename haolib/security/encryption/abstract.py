"""Encryption base."""

from typing import Any, Protocol


class AbstractEncryptor(Protocol):
    """Abstract encryptor."""

    def encrypt(self, data: bytes, *args: Any, **kwargs: Any) -> bytes:
        """Encrypt data."""
        ...

    def decrypt(self, data: bytes, *args: Any, **kwargs: Any) -> bytes:
        """Decrypt data."""
        ...
