"""Abstract data target protocol.

Defines the interface for any target that can execute operations.
This includes storages, ML models, APIs, and other data processing targets.
"""

from typing import Any, Protocol, TypeVar

from haolib.storages.operations.base import Operation, Pipeline

T_Result = TypeVar("T_Result")


class AbstractDataTarget(Protocol):
    """Protocol for any target that can execute operations.

    This includes storages, ML models, APIs, and other data processing targets.
    The ^ operator semantically means "send data to this target".

    Example:
        ```python
        # Storage target
        result = await (reado(...) ^ sql_storage).execute()

        # ML Model target (future)
        result = await (reado(...) ^ ml_model).execute()

        # API target (future)
        result = await (reado(...) ^ api_client).execute()
        ```

    """

    async def execute[T_Result](
        self,
        operation: Operation[Any, T_Result] | Pipeline[Any, Any, T_Result],
    ) -> T_Result:
        """Execute operation or pipeline.

        Target analyzes the operation/pipeline and executes it.
        Different targets may have different execution strategies:
        - Storages: execute in database, optimize queries, etc.
        - ML Models: run predictions, batch processing, etc.
        - APIs: send requests, handle responses, etc.

        Args:
            operation: Operation or pipeline to execute.

        Returns:
            Result of execution.

        Raises:
            TargetError: If target operation fails.
            TypeError: If operation type is not supported.

        """
        ...
