"""Idempotency keys storage base."""

from typing import Protocol


class AbstractIdempotencyKeysStorage(Protocol):
    """Base idempotency keys storage."""

    async def is_processed(self, idempotency_key: str) -> bool:
        """Check if the idempotency key is processed.

        Args:
            idempotency_key: The idempotency key.

        Returns:
            bool: True if the idempotency key is processed, False otherwise.

        """
        ...

    async def set_processed(self, idempotency_key: str) -> None:
        """Set the idempotency key as processed.

        Args:
            idempotency_key: The idempotency key.

        """
        ...
