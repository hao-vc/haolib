"""Native batches."""

from collections.abc import Iterator
from typing import Self

from haolib.batches.base import BaseBatch
from haolib.entities.base import BaseEntity


class EntityBatch[T_Id, T_Entity: BaseEntity](BaseBatch[T_Id, T_Entity]):
    """Entity batch."""

    def __init__(self, data: list[T_Entity]) -> None:
        """Initialize the batch."""

        self._entities: dict[T_Id, T_Entity] = {}
        self._entities_list_indexed: list[T_Id] = []

        for entity in data:
            self._entities[entity.id] = entity
            self._entities_list_indexed.append(entity.id)

    async def __iter__(self) -> Iterator[T_Entity]:
        """Iterate over the batch."""
        return iter(self._entities.values())

    async def add_dict_data(self, data: dict[T_Id, T_Entity]) -> Self:
        """Return the batch from a dict."""

        for entity_id, entity in data.items():
            self._entities[entity_id] = entity
            self._entities_list_indexed.append(entity_id)

        return self

    async def add_list_data(self, data: list[T_Entity]) -> Self:
        """Return the batch from a list."""

        for entity in data:
            self._entities[entity.id] = entity
            self._entities_list_indexed.append(entity.id)

        return self

    async def add_set_data(self, data: set[T_Entity]) -> Self:
        """Return the batch from a set."""
        for entity in data:
            self._entities[entity.id] = entity
            self._entities_list_indexed.append(entity.id)

        return self

    async def to_dict(self) -> dict[T_Id, T_Entity]:
        """Return the batch as a dict."""
        return self._entities

    async def to_list(self) -> list[T_Entity]:
        """Return the batch as a list."""
        return [self._entities[entity_index] for entity_index in self._entities_list_indexed]

    async def to_set(self) -> set[T_Entity]:
        """Return the batch as a set."""
        return set(self._entities.values())

    async def get_first(self, exception: Exception | type[Exception]) -> T_Entity:
        """Return the first item in the batch."""
        if not self._entities_list_indexed:
            raise exception

        return self._entities[self._entities_list_indexed[0]]

    async def get_ids(self) -> set[T_Id]:
        """Return the ids of the batch."""
        return set(self._entities.keys())

    async def get_size(self) -> int:
        """Return the size of the batch."""

        return len(self._entities_list_indexed)

    async def get_unique_size(self) -> int:
        """Return the unique size of the batch."""

        return len(self._entities)
