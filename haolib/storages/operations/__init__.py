"""Storage operations.

This module re-exports pipeline operations for backward compatibility.
New code should import from haolib.pipelines directly.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from haolib.pipelines import (
        CreateOperation,
        DeleteOperation,
        ExecutablePipelineExecutor,
        FilterOperation,
        MapOperation,
        Operation,
        PatchOperation,
        Pipeline,
        PipelineAnalysis,
        PipelineOptimizer,
        PipelineValidationError,
        PipelineValidator,
        ReadOperation,
        ReduceOperation,
        TargetBoundOperation,
        TargetSwitch,
        TransformOperation,
        UpdateOperation,
    )

# For runtime, import lazily to avoid circular import
def __getattr__(name: str) -> Any:
    """Lazy import for backward compatibility."""
    import haolib.pipelines as pipelines  # noqa: PLC0415
    
    return getattr(pipelines, name)

__all__ = [
    "CreateOperation",
    "DeleteOperation",
    "ExecutablePipelineExecutor",
    "FilterOperation",
    "MapOperation",
    "Operation",
    "PatchOperation",
    "Pipeline",
    "PipelineAnalysis",
    "PipelineOptimizer",
    "PipelineValidationError",
    "PipelineValidator",
    "ReadOperation",
    "ReduceOperation",
    "SQLAlchemyPipelineOptimizer",
    "TargetBoundOperation",
    "TargetSwitch",
    "TransformOperation",
    "UpdateOperation",
]

# Import SQLAlchemyPipelineOptimizer lazily to avoid circular dependency
def _get_sqlalchemy_optimizer() -> Any:
    """Lazy import for SQLAlchemyPipelineOptimizer."""
    from haolib.storages.operations.optimizer.sqlalchemy import SQLAlchemyPipelineOptimizer  # noqa: PLC0415
    return SQLAlchemyPipelineOptimizer

# Make SQLAlchemyPipelineOptimizer available via __getattr__
def __getattr__(name: str) -> Any:
    """Lazy import for backward compatibility."""
    if name == "SQLAlchemyPipelineOptimizer":
        return _get_sqlalchemy_optimizer()
    
    import haolib.pipelines as pipelines  # noqa: PLC0415
    
    return getattr(pipelines, name)
