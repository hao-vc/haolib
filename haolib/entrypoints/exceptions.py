"""Exceptions for the entrypoints."""


class EntrypointsInconsistencyError(Exception):
    """Entrypoints inconsistency error."""

    def __init__(self, message: str) -> None:
        """Initialize the entrypoints inconsistency error."""
        super().__init__(message)
