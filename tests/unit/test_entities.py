"""Test entities."""

from typing import Self

import pytest

from haolib.batches.batch import Batch
from haolib.entities.base import BaseEntity
from haolib.entities.base.create import BaseBulkEntityCreate, BaseEntityCreate
from haolib.entities.base.read import BaseBulkEntityRead, BaseEntityRead
from haolib.entities.base.update import BaseBulkEntityUpdate, BaseEntityUpdate


class CounterGenerator:
    """Counter generator."""

    def __init__(self) -> None:
        self.counter = 0

    def generate(self) -> int:
        """Generate a new counter."""
        self.counter += 1
        return self.counter


class Entity(BaseEntity[int]):
    """Entity."""

    id: int
    name: str

    def __init__(self, id: int, name: str) -> None:
        self.id = id
        self.name = name

    def __eq__(self, value: object) -> bool:
        """Check if the entity is equal to another entity."""
        if not isinstance(value, Entity):
            return False

        return self.id == value.id

    def __hash__(self) -> int:
        """Hash the entity."""
        return hash((self.id,))


class EntityRead(BaseEntityRead[int, Entity]):
    """Entity read."""

    id: int

    def __init__(self, id: int) -> None:
        self.id = id

    def __eq__(self, value: object) -> bool:
        """Check if the entity is equal to another entity."""
        if not isinstance(value, EntityRead):
            return False

        return self.id == value.id

    def __hash__(self) -> int:
        """Hash the entity."""
        return hash((self.id,))

    @classmethod
    async def from_entity(cls, entity: Entity) -> Self:
        """Get entity read from entity."""
        return cls(id=entity.id)


class EntitiesBulkRead(BaseBulkEntityRead[int, Entity, EntityRead]):
    """Entities bulk read."""

    entities: list[EntityRead]

    def __init__(self, entities: list[EntityRead]) -> None:
        self.entities = entities

    @classmethod
    async def from_batch(cls, batch: Batch[int, Entity]) -> Self:
        """Get entities bulk read from batch."""
        return cls(entities=[EntityRead(id=entity.id) for entity in batch.to_list()])


class EntityUpdate(BaseEntityUpdate[int, Entity]):
    """Entity update."""

    id: int
    name: str

    def __init__(self, id: int, name: str) -> None:
        self.id = id
        self.name = name

    async def update_entity(self, entity: Entity) -> Entity:
        """Update entity and return the updated entity."""
        entity.name = self.name

        return entity


class EntitiesBulkUpdate(BaseBulkEntityUpdate[int, Entity, EntityUpdate]):
    """Entities bulk update."""

    entities: list[EntityUpdate]

    def __init__(self, entities: list[EntityUpdate]) -> None:
        self.entities = entities

    async def get_entity_updates(self) -> list[EntityUpdate]:
        """Get entities."""
        return self.entities


class EntityCreate(BaseEntityCreate[int, Entity]):
    """Entity create."""

    name: str

    def __init__(self, name: str) -> None:
        self.name = name

    async def create_entity(self, id_generator: CounterGenerator) -> Entity:
        """Create entity."""
        return Entity(id=id_generator.generate(), name=self.name)


class EntitiesBulkCreate(BaseBulkEntityCreate[int, Entity, EntityCreate]):
    """Entities bulk create."""

    entities: list[EntityCreate]

    def __init__(self, entities: list[EntityCreate]) -> None:
        self.entities = entities

    async def get_entity_creates(self) -> list[EntityCreate]:
        """Get entities."""
        return self.entities


@pytest.mark.asyncio
async def test_entity_read() -> None:
    """Test entity read."""
    entity = EntityRead(id=1)
    assert entity.id == 1


@pytest.mark.asyncio
async def test_entities_bulk_read() -> None:
    """Test entities bulk read."""
    batch = Batch[int, Entity](key_getter=lambda entity: entity.id).merge_list(
        [Entity(id=1, name="test"), Entity(id=2, name="test"), Entity(id=3, name="test")]
    )
    entities_bulk_read = await EntitiesBulkRead.from_batch(batch)
    assert entities_bulk_read.entities == [EntityRead(id=1), EntityRead(id=2), EntityRead(id=3)]


@pytest.mark.asyncio
async def test_entities_bulk_update() -> None:
    """Test entities bulk update."""
    batch = Batch[int, Entity](key_getter=lambda entity: entity.id).merge_list(
        [Entity(id=1, name="test"), Entity(id=2, name="test"), Entity(id=3, name="test")]
    )
    update_batch = EntitiesBulkUpdate(
        entities=[
            EntityUpdate(id=1, name="test2"),
            EntityUpdate(id=2, name="test2"),
            EntityUpdate(id=3, name="test2"),
        ]
    )
    updated_batch = await update_batch.update_batch(batch)
    assert updated_batch.to_list() == [
        Entity(id=1, name="test2"),
        Entity(id=2, name="test2"),
        Entity(id=3, name="test2"),
    ]
    assert updated_batch.to_dict() == {
        1: Entity(id=1, name="test2"),
        2: Entity(id=2, name="test2"),
        3: Entity(id=3, name="test2"),
    }
    assert updated_batch.to_set() == {
        Entity(id=1, name="test2"),
        Entity(id=2, name="test2"),
        Entity(id=3, name="test2"),
    }


@pytest.mark.asyncio
async def test_entities_bulk_create() -> None:
    """Test entities bulk create and return batch of the created entities."""
    create_batch = EntitiesBulkCreate(
        entities=[EntityCreate(name="test"), EntityCreate(name="test"), EntityCreate(name="test")]
    )
    id_generator = CounterGenerator()
    created_batch = await create_batch.create_batch(id_generator)
    assert created_batch.to_list() == [
        Entity(id=1, name="test"),
        Entity(id=2, name="test"),
        Entity(id=3, name="test"),
    ]
