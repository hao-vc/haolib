"""SQLAlchemy batches."""

from typing import TYPE_CHECKING, Any, Self

from haolib.batches.batch import Batch
from haolib.batches.entities import EntityBatch
from haolib.entities.base import BaseEntity
from haolib.models.entities import BaseEntityModel

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import ScalarResult
    from sqlalchemy.ext.asyncio import AsyncSession


class SQLAlchemyEntityModelBatch[T_Id, T_Model: BaseEntityModel, T_Entity: BaseEntity](Batch[T_Id, T_Model]):
    """SQLAlchemy entity model batch."""

    def __init__(
        self, model_class: type[T_Model], key_getter: Callable[[T_Model], T_Id] = lambda model: model.id
    ) -> None:
        """Initialize the batch."""

        super().__init__(key_getter)
        self._model_class = model_class

    async def merge_batch_to_db(self, session: AsyncSession) -> Self:
        """Merge the batch to the database."""

        for model in self.to_list():
            await session.merge(model)

        return self

    def from_scalars(self, scalars: ScalarResult[T_Model]) -> Self:
        """Return the batch from scalars."""

        self.merge_list(list(scalars.all()))

        return self

    def update_from_entity_batch(self, entity_batch: EntityBatch[T_Id, T_Entity], *args: Any, **kwargs: Any) -> Self:
        """Update the batch from entity batch."""

        for entity in entity_batch:
            self.get_by_key(entity.id, exception=ValueError).update_from_entity(entity, *args, **kwargs)

        return self

    def from_entity_batch(self, entity_batch: EntityBatch[T_Id, T_Entity], *args: Any, **kwargs: Any) -> Self:
        """Return the batch from entity batch."""

        self.merge_list([self._model_class.from_entity(entity, *args, **kwargs) for entity in entity_batch])

        return self

    def to_entity_batch(self, *args: Any, **kwargs: Any) -> EntityBatch[T_Id, T_Entity]:
        """Return the entity batch from model batch."""

        return EntityBatch().merge_list(
            [self.get_by_key(model.id, exception=ValueError).to_entity(*args, **kwargs) for model in self]
        )
