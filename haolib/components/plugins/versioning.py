"""Version checking utilities for plugin compatibility."""

from packaging import version as packaging_version
from packaging.version import InvalidVersion

from haolib.components.abstract import ComponentInconsistencyError


class PluginVersionError(ComponentInconsistencyError):
    """Plugin version compatibility error.

    Raised when a plugin's version requirements are not satisfied by the component version.
    """


def check_version_compatibility(
    component_version: str,
    plugin_name: str,
    min_version: str | None = None,
    max_version: str | None = None,
) -> None:
    """Check if component version satisfies plugin requirements.

    Args:
        component_version: The component version to check.
        plugin_name: Name of the plugin for error messages.
        min_version: Minimum required component version (inclusive).
        max_version: Maximum allowed component version (exclusive).

    Raises:
        PluginVersionError: If version requirements are not satisfied.
        ValueError: If version strings are invalid.

    """
    try:
        comp_ver = packaging_version.parse(component_version)
    except InvalidVersion as e:
        msg = f"Invalid component version format: {component_version}"
        raise ValueError(msg) from e

    if min_version is not None:
        try:
            min_ver = packaging_version.parse(min_version)
        except InvalidVersion as e:
            msg = f"Invalid min_component_version format: {min_version}"
            raise ValueError(msg) from e

        if comp_ver < min_ver:
            msg = (
                f"Plugin '{plugin_name}' requires component version >= {min_version}, "
                f"but component version is {component_version}"
            )
            raise PluginVersionError(msg)

    if max_version is not None:
        try:
            max_ver = packaging_version.parse(max_version)
        except InvalidVersion as e:
            msg = f"Invalid max_component_version format: {max_version}"
            raise ValueError(msg) from e

        if comp_ver >= max_ver:
            msg = (
                f"Plugin '{plugin_name}' requires component version < {max_version}, "
                f"but component version is {component_version}"
            )
            raise PluginVersionError(msg)
