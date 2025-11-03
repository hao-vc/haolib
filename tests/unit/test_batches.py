"""Test batches."""

import pytest

from haolib.batches.batch import Batch


class Entity(Batch[int, int]):
    """Entity."""

    id: int

    def __init__(self, id: int) -> None:
        self.id = id

    def __eq__(self, value: object) -> bool:
        """Check if the entity is equal to another entity."""
        if not isinstance(value, Entity):
            return False

        return self.id == value.id

    def __hash__(self) -> int:
        """Hash the entity."""
        return hash((self.id,))


@pytest.mark.asyncio
async def test_entity_batch_add_dict_data() -> None:
    """Test entity batch add dict data."""
    batch = Batch[int, Entity](key_getter=lambda entity: entity.id).merge_dict(
        {1: Entity(id=1), 2: Entity(id=2), 3: Entity(id=3)}
    )
    assert batch.to_dict() == {1: Entity(id=1), 2: Entity(id=2), 3: Entity(id=3)}


@pytest.mark.asyncio
async def test_entity_batch_add_list_data() -> None:
    """Test entity batch add list data."""
    batch = Batch[int, Entity](key_getter=lambda entity: entity.id).merge_list(
        [Entity(id=1), Entity(id=2), Entity(id=3)]
    )
    assert batch.to_list() == [Entity(id=1), Entity(id=2), Entity(id=3)]


@pytest.mark.asyncio
async def test_entity_batch_add_set_data() -> None:
    """Test entity batch add set data."""
    batch = Batch[int, Entity](key_getter=lambda entity: entity.id).merge_set(
        {Entity(id=1), Entity(id=2), Entity(id=3)}
    )
    assert batch.to_list() == [Entity(id=1), Entity(id=2), Entity(id=3)]


@pytest.mark.asyncio
async def test_entity_batch_to_dict() -> None:
    """Test entity batch to dict."""
    batch = Batch[int, Entity](key_getter=lambda entity: entity.id).merge_list(
        [Entity(id=1), Entity(id=2), Entity(id=3)]
    )
    assert batch.to_dict() == {1: Entity(id=1), 2: Entity(id=2), 3: Entity(id=3)}


@pytest.mark.asyncio
async def test_entity_batch_to_list() -> None:
    """Test entity batch to list."""
    batch = Batch[int, Entity](key_getter=lambda entity: entity.id).merge_list(
        [Entity(id=1), Entity(id=2), Entity(id=3)]
    )
    assert batch.to_list() == [Entity(id=1), Entity(id=2), Entity(id=3)]


@pytest.mark.asyncio
async def test_entity_batch_to_set() -> None:
    """Test entity batch to set."""
    batch = Batch[int, Entity](key_getter=lambda entity: entity.id).merge_list(
        [Entity(id=1), Entity(id=2), Entity(id=3)]
    )
    assert batch.to_set() == {Entity(id=1), Entity(id=2), Entity(id=3)}


@pytest.mark.asyncio
async def test_entity_batch_get_first() -> None:
    """Test entity batch get first."""
    batch = Batch[int, Entity](key_getter=lambda entity: entity.id).merge_list(
        [Entity(id=1), Entity(id=2), Entity(id=3)]
    )
    assert batch.get_by_index(0, exception=Exception) == Entity(id=1)


@pytest.mark.asyncio
async def test_entity_batch_get_size() -> None:
    """Test entity batch get size."""
    batch = Batch[int, Entity](key_getter=lambda entity: entity.id).merge_list(
        [Entity(id=1), Entity(id=2), Entity(id=3)]
    )
    batch_size = 3
    assert len(batch) == batch_size


@pytest.mark.asyncio
async def test_entity_batch_get_unique_size() -> None:
    """Test entity batch get unique size."""
    batch = Batch[int, Entity](key_getter=lambda entity: entity.id).merge_list(
        [Entity(id=1), Entity(id=2), Entity(id=3)]
    )
    batch_size = 3
    assert len(batch) == batch_size
