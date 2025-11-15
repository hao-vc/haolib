"""Abstract plugin protocol."""

from typing import TYPE_CHECKING

from haolib.components.plugins.abstract import AbstractPlugin, AbstractPluginPreset

if TYPE_CHECKING:
    from haolib.entrypoints.abstract import AbstractEntrypoint


class AbstractEntrypointPlugin[T_Entrypoint: AbstractEntrypoint](AbstractPlugin[T_Entrypoint]):
    """Abstract entrypoint plugin protocol."""


class AbstractEntrypointPluginPreset[
    T_Entrypoint: AbstractEntrypoint,
    T_Plugin: AbstractEntrypointPlugin,
](AbstractPluginPreset[T_Entrypoint, T_Plugin]):
    """Abstract entrypoint plugin preset protocol."""
