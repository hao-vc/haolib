"""UUIDv7 generator."""

from uuid import UUID, uuid4


def get_random_uuid() -> UUID:
    """Get a random UUID."""

    return uuid4()
