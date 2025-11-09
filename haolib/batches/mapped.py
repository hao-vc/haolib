"""Mapped batches."""

from typing import TYPE_CHECKING, Any, Self

from haolib.batches.abstract import AbstractBatch
from haolib.batches.batch import Batch
from haolib.database.models.mapped.abstract import AbstractMappedModel, AbstractUpdateableMappedModel

if TYPE_CHECKING:
    from collections.abc import Callable


class MappedBatch[T_Key, T_Mapped: AbstractMappedModel, T_MappedTo](Batch[T_Key, T_Mapped]):
    """Mapped model batch."""

    def __init__(
        self,
        key_getter: Callable[[T_Mapped], T_Key],
    ) -> None:
        """Initialize the batch."""

        super().__init__(key_getter=key_getter)

    def merge_from_batch(
        self, batch: AbstractBatch[T_Key, T_MappedTo], mapped_class: type[T_Mapped], *args: Any, **kwargs: Any
    ) -> Self:
        """Merge the values from the given batch to this batch.

        Args:
            batch: The batch to merge from.
            mapped_class: The mapped class to use to create the mapped values.
            *args: The arguments to pass to the mapped class.
            **kwargs: The keyword arguments to pass to the mapped class.

        Returns:
            Self: The updated batch.

        """

        self.merge_list([mapped_class.create_from(mapped, *args, **kwargs) for mapped in batch])

        return self

    def merge_to_batch[T_Batch: AbstractBatch](self, batch: T_Batch, *args: Any, **kwargs: Any) -> T_Batch:
        """Merge the values from this batch to the given batch.

        Args:
            batch: The batch to merge to.
            *args: The arguments to pass to the mapped class.
            **kwargs: The keyword arguments to pass to the mapped class.

        Returns:
            AbstractBatch[T_Key, T_MappedTo]: The updated batch.

        """

        return batch.merge_list(
            [self.get_by_key(self.key_getter(mapped), exception=ValueError).convert(*args, **kwargs) for mapped in self]
        )


class UpdateableMappedBatch[T_Key, T_Mapped: AbstractUpdateableMappedModel, T_MappedTo](
    MappedBatch[T_Key, T_Mapped, T_MappedTo]
):
    """Updateable from mapped batch."""

    def update_from_batch(self, batch: AbstractBatch[T_Key, T_MappedTo], *args: Any, **kwargs: Any) -> Self:
        """Update this batch from the given batch.

        Args:
            batch: The batch to update from.
            *args: The arguments to pass to the mapped class.
            **kwargs: The keyword arguments to pass to the mapped class.

        Returns:
            Self: The updated batch.

        """

        for mapped in batch:
            self.get_by_key(batch.key_getter(mapped), exception=ValueError).update_from(mapped, *args, **kwargs)

        return self
