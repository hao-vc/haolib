"""SQLAlchemy batches."""

from collections.abc import Iterator
from typing import Any, Self

from sqlalchemy import ScalarResult
from sqlalchemy.ext.asyncio import AsyncSession

from haolib.batches.base import BaseBatch
from haolib.batches.entities import EntityBatch
from haolib.entities.base import BaseEntity
from haolib.models.entities import BaseEntityModel


class SQLAlchemyEntityModelBatch[T_Id, T_Model: BaseEntityModel, T_Entity: BaseEntity](BaseBatch[T_Id, T_Model]):
    """SQLAlchemy entity model batch."""

    def __init__(self, data: list[T_Model]) -> None:
        """Initialize the batch."""

        self._models_list_indexed: list[T_Id] = []
        self._models: dict[T_Id, T_Model] = {}

        for model in data:
            self._models[model.id] = model
            self._models_list_indexed.append(model.id)

        self._index = 0

    def __iter__(self) -> Iterator[T_Model]:
        """Iterate over the batch."""

        self._index = 0

        return self

    def __next__(self) -> T_Model:
        """Return the next item in the batch."""

        if self._index >= len(self._models_list_indexed):
            raise StopIteration

        result = self._models[self._models_list_indexed[self._index]]

        self._index += 1

        return result

    async def add_dict_data(self, data: dict[T_Id, T_Model]) -> Self:
        """Return the batch from a dict."""

        for model_id, model in data.items():
            self._models[model_id] = model
            self._models_list_indexed.append(model_id)

        return self

    async def add_list_data(self, data: list[T_Model]) -> Self:
        """Return the batch from a list."""

        for model in data:
            self._models[model.id] = model
            self._models_list_indexed.append(model.id)

        return self

    async def add_set_data(self, data: set[T_Model]) -> Self:
        """Return the batch from a set."""
        for model in data:
            self._models[model.id] = model
            self._models_list_indexed.append(model.id)

        return self

    async def to_dict(self) -> dict[T_Id, T_Model]:
        """Return the batch as a dict."""
        return self._models

    async def to_list(self) -> list[T_Model]:
        """Return the batch as a list."""
        return [self._models[model_index] for model_index in self._models_list_indexed]

    async def to_set(self) -> set[T_Model]:
        """Return the batch as a set."""
        return set(self._models.values())

    async def get_first(self, exception: Exception | type[Exception]) -> T_Model:
        """Return the first item in the batch."""
        if not self._models_list_indexed:
            raise exception

        return self._models[self._models_list_indexed[0]]

    async def get_by_id(self, id: T_Id) -> T_Model:
        """Return the item by id."""
        return self._models[id]

    async def get_ids(self) -> set[T_Id]:
        """Return the ids of the batch."""
        return set(self._models.keys())

    async def get_size(self) -> int:
        """Return the size of the batch."""

        return len(self._models_list_indexed)

    async def get_unique_size(self) -> int:
        """Return the unique size of the batch."""

        return len(self._models)

    async def merge_batch_to_db(self, session: AsyncSession) -> Self:
        """Merge the batch to the database."""

        for model in self._models.values():
            await session.merge(model)

        return self

    def from_scalars(self, scalars: ScalarResult[T_Model]) -> Self:
        """Return the batch from scalars."""

        for scalar in scalars.all():
            self._models[scalar.id] = scalar
            self._models_list_indexed.append(scalar.id)

        return self

    def update_from_entity_batch(self, entity_batch: EntityBatch[T_Id, T_Entity], *args: Any, **kwargs: Any) -> Self:
        """Update the batch from entity batch."""

        for entity in entity_batch:
            self._models[entity.id] = self._models[entity.id].update_from_entity(entity, *args, **kwargs)

        return self

    def from_entity_batch(
        self, entity_batch: EntityBatch[T_Id, T_Entity], model_class: type[T_Model], *args: Any, **kwargs: Any
    ) -> Self:
        """Return the batch from entity batch."""

        for entity in entity_batch:
            self._models[entity.id] = model_class.from_entity(entity, *args, **kwargs)
            self._models_list_indexed.append(entity.id)

        return self

    def to_entity_batch(self, *args: Any, **kwargs: Any) -> EntityBatch[T_Id, T_Entity]:
        """Return the entity batch from model batch."""

        return EntityBatch(
            [self._models[model_id].to_entity(*args, **kwargs) for model_id in self._models_list_indexed]
        )
