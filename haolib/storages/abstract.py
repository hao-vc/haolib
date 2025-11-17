"""Abstract storages."""

from typing import Any, Protocol, TypeVar

from haolib.components.abstract import AbstractComponent
from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.pipelines.base import Operation, Pipeline
from haolib.pipelines.context import PipelineContext
from haolib.storages.plugins.abstract import AbstractStoragePlugin, AbstractStoragePluginPreset
from haolib.storages.targets.abstract import AbstractDataTarget

T_Result = TypeVar("T_Result")


class AbstractStorage(
    AbstractComponent[AbstractStoragePlugin, AbstractStoragePluginPreset],
    AbstractDataTarget,
    Protocol,
):
    """Abstract storage protocol.

    Provides universal interface for storage operations with pipeline support.
    Storage implementations handle execution of operations, potentially optimizing
    them for execution on storage side (SQL, aggregation pipelines, etc.).

    Each operation or pipeline is executed atomically. For transactional storages
    (like SQL databases), operations are automatically wrapped in transactions.
    For non-transactional storages (like S3), operations execute immediately.
    """

    @property
    def data_type_registry(self) -> DataTypeRegistry:
        """Get the data type registry."""
        ...

    async def execute[T_Result](
        self,
        operation: Operation[Any, T_Result] | Pipeline[Any, Any, T_Result],
        previous_result: Any = None,
        pipeline_context: PipelineContext | None = None,
    ) -> T_Result:
        """Execute operation or pipeline atomically.

        Storage analyzes the operation/pipeline and executes it optimally.
        It may optimize the pipeline to execute on storage side (e.g., single SQL query)
        or execute it in Python code.

        Each operation or pipeline is executed atomically:
        - For transactional storages: automatically wrapped in a transaction
        - For non-transactional storages: operations execute immediately

        To execute multiple operations in a single transaction, compose them into a Pipeline.

        Args:
            operation: Operation or pipeline to execute.
            previous_result: Optional result from previous operation (for pipeline mode).
            pipeline_context: Optional context about the entire pipeline for global optimization.

        Returns:
            Result of execution.

        Raises:
            StorageError: If storage operation fails.
            TypeError: If operation type is not supported.

        Example:
            ```python
            from haolib.pipelines import filtero
            from haolib.storages.indexes.params import ParamIndex

            # Simple operation (executed atomically)
            await storage.create([user1, user2]).returning().execute()

            # Pipeline (all operations in single transaction)
            user_index = ParamIndex(User, age=18)
            pipeline = (
                storage.create([user1, user2]).returning()
                | storage.read(user_index).returning()
                | filtero(lambda u: u.age >= 18)
            )
            results = await pipeline.execute()
            ```

        """
        ...
