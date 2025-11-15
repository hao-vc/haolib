"""Storage operations."""

from haolib.storages.operations.base import (
    Operation,
    Pipeline,
    TargetBoundOperation,
    TargetSwitch,
)
from haolib.storages.operations.concrete import (
    CreateOperation,
    DeleteOperation,
    FilterOperation,
    MapOperation,
    ReadOperation,
    ReduceOperation,
    TransformOperation,
    UpdateOperation,
)
from haolib.storages.operations.executor import ExecutablePipelineExecutor
from haolib.storages.operations.optimizer import PipelineAnalysis, PipelineOptimizer
from haolib.storages.operations.optimizer.sqlalchemy import SQLAlchemyPipelineOptimizer
from haolib.storages.operations.validator import PipelineValidationError, PipelineValidator

__all__ = [
    "CreateOperation",
    "DeleteOperation",
    "ExecutablePipelineExecutor",
    "FilterOperation",
    "MapOperation",
    "Operation",
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
