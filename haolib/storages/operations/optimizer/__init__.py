"""Pipeline optimizers for storage backends.

This module re-exports optimizer types from haolib.pipelines
for backward compatibility.
New code should import from haolib.pipelines directly.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from haolib.pipelines import PipelineAnalysis, PipelineOptimizer

# For runtime, import lazily to avoid circular import
def __getattr__(name: str) -> Any:
    """Lazy import for backward compatibility."""
    import haolib.pipelines as pipelines  # noqa: PLC0415
    
    return getattr(pipelines, name)

__all__ = [
    "PipelineAnalysis",
    "PipelineOptimizer",
]
