"""Pipelines for data processing."""

from haolib.pipelines.base import (
    Operation,
    Pipeline,
    TargetBoundOperation,
    TargetSwitch,
)
from haolib.pipelines.context import PipelineContext
from haolib.pipelines.dsl import (
    filtero,
    mapo,
    reduceo,
    transformo,
)
from haolib.pipelines.executor import ExecutablePipelineExecutor
from haolib.pipelines.operations import (
    CreateOperation,
    DeleteOperation,
    FilterOperation,
    MapOperation,
    PatchOperation,
    ReadOperation,
    ReduceOperation,
    TransformOperation,
    UpdateOperation,
)
from haolib.pipelines.optimizer import PipelineAnalysis, PipelineOptimizer
from haolib.pipelines.validator import PipelineValidationError, PipelineValidator

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
    "PipelineContext",
    "PipelineOptimizer",
    "PipelineValidationError",
    "PipelineValidator",
    "ReadOperation",
    "ReduceOperation",
    "TargetBoundOperation",
    "TargetSwitch",
    "TransformOperation",
    "UpdateOperation",
    "filtero",
    "mapo",
    "reduceo",
    "transformo",
]
