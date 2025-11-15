"""Abstract operations."""

from typing import Protocol

from haolib.storages.abstract import AbstractStorage


class AbstractStorageOperation[T_Storage: AbstractStorage](Protocol):
    """Abstract storage operation protocol."""

    storage: T_Storage
