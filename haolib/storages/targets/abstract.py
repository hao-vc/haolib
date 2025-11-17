"""Abstract data target protocol.

Defines the interface for any target that can execute operations.
This includes storages, ML models, APIs, and other data processing targets.
"""

from typing import Any, Protocol, TypeVar

from haolib.pipelines.base import Operation, Pipeline
from haolib.pipelines.context import PipelineContext

T_Result = TypeVar("T_Result")


class AbstractDataTarget(Protocol):
    """Protocol for any target that can execute operations.

    This includes storages, ML models, APIs, and other data processing targets.
    The ^ operator semantically means "send data to this target".

    Example:
        ```python
        from haolib.storages.indexes.params import ParamIndex

        # Storage target (using fluent API)
        result = await sql_storage.read(ParamIndex(User)).returning().execute()

        # ML Model target (future)
        # result = await ml_model.predict(...).execute()

        # API target (future)
        # result = await api_client.get(...).execute()
        ```

    """

    async def execute[T_Result](
        self,
        operation: Operation[Any, T_Result] | Pipeline[Any, Any, T_Result],
        previous_result: Any = None,
        pipeline_context: PipelineContext | None = None,
    ) -> T_Result:
        """Execute operation or pipeline.

        Target analyzes the operation/pipeline and executes it.
        Different targets may have different execution strategies:
        - Storages: execute in database, optimize queries, etc.
        - ML Models: run predictions, batch processing, etc.
        - APIs: send requests, handle responses, etc.

        Args:
            operation: Operation or pipeline to execute.
            previous_result: Optional result from previous operation (for pipeline mode).
            pipeline_context: Optional context about the entire pipeline for global optimization.

        Returns:
            Result of execution.

        Raises:
            TargetError: If target operation fails.
            TypeError: If operation type is not supported.

        """
        ...
