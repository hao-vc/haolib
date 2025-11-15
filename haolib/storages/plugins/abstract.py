"""Abstract storage plugins."""

from typing import TYPE_CHECKING

from haolib.components.plugins.abstract import AbstractPlugin, AbstractPluginPreset

if TYPE_CHECKING:
    from haolib.storages.abstract import AbstractStorage


class AbstractStoragePlugin[T_Storage: AbstractStorage](AbstractPlugin[T_Storage]):
    """Abstract entrypoint plugin protocol."""


class AbstractStoragePluginPreset[
    T_Storage: AbstractStorage,
    T_Plugin: AbstractStoragePlugin,
](AbstractPluginPreset[T_Storage, T_Plugin]):
    """Abstract storage plugin preset protocol."""
