"""Native batches."""

from haolib.batches.batch import Batch
from haolib.entities.base import BaseEntity


class EntityBatch[T_Id, T_Entity: BaseEntity](Batch[T_Id, T_Entity]):
    """Entity batch.

    This batch is a batch of entities that are indexed by their ID.
    """

    def __init__(self) -> None:
        """Initialize the batch."""

        super().__init__(lambda entity: entity.id)
