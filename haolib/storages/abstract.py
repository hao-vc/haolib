"""Abstract storages."""

from typing import Any, Protocol, TypeVar

from haolib.components.abstract import AbstractComponent
from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.operations.base import Operation, Pipeline
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

        Returns:
            Result of execution.

        Raises:
            StorageError: If storage operation fails.
            TypeError: If operation type is not supported.

        Example:
            ```python
            from haolib.storages.dsl import createo, reado, filtero
            from haolib.storages.indexes import index

            # Simple operation (executed atomically)
            await storage.execute(createo([user1, user2]))

            # Pipeline (all operations in single transaction)
            user_index = index(User, age=18)
            pipeline = (
                createo([user1, user2])
                | reado(search_index=user_index)
                | filtero(lambda u: u.age >= 18)
            )
            results = await storage.execute(pipeline)
            ```

        """
        ...
