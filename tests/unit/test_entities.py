"""Test entities."""

from typing import Self

import pytest

from haolib.batches.entities import EntityBatch
from haolib.entities.base import BaseEntity
from haolib.entities.create import BaseBulkEntityCreate, BaseEntityCreate
from haolib.entities.get import BaseBulkEntityGet, BaseEntityGet
from haolib.entities.update import BaseBulkEntityUpdate, BaseEntityUpdate


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


class EntityGet(BaseEntityGet[int, Entity]):
    """Entity get."""

    id: int

    def __init__(self, id: int) -> None:
        self.id = id

    def __eq__(self, value: object) -> bool:
        """Check if the entity is equal to another entity."""
        if not isinstance(value, EntityGet):
            return False

        return self.id == value.id

    def __hash__(self) -> int:
        """Hash the entity."""
        return hash((self.id,))

    @classmethod
    async def from_entity(cls, entity: Entity) -> Self:
        """Get entity get from entity."""
        return cls(id=entity.id)


class EntitiesBulkGet(BaseBulkEntityGet[int, Entity, EntityGet]):
    """Entities bulk get."""

    entities: list[EntityGet]

    def __init__(self, entities: list[EntityGet]) -> None:
        self.entities = entities

    @classmethod
    async def from_batch(cls, batch: EntityBatch[int, Entity]) -> Self:
        """Get entities bulk get from batch."""
        return cls(entities=[EntityGet(id=entity.id) for entity in batch.to_list()])


class EntityUpdate(BaseEntityUpdate[int, Entity]):
    """Entity update."""

    id: int
    name: str

    def __init__(self, id: int, name: str) -> None:
        self.id = id
        self.name = name

    async def update_entity(self, entity: Entity) -> Entity:
        """Update entity."""
        entity.name = self.name

        return entity


class EntitiesBulkUpdate(BaseBulkEntityUpdate[int, Entity, EntityUpdate]):
    """Entities bulk update."""

    entities: list[EntityUpdate]

    def __init__(self, entities: list[EntityUpdate]) -> None:
        self.entities = entities


class EntityCreate(BaseEntityCreate[int, Entity]):
    """Entity create."""

    name: str

    def __init__(self, name: str) -> None:
        self.name = name

    async def create_entity(self, id: int) -> Entity:
        """Create entity."""
        return Entity(id=id, name=self.name)


class EntitiesBulkCreate(BaseBulkEntityCreate[int, Entity, EntityCreate]):
    """Entities bulk create."""

    entities: list[EntityCreate]

    def __init__(self, entities: list[EntityCreate]) -> None:
        self.entities = entities

    async def create_batch(self) -> EntityBatch[int, Entity]:
        """Create batch."""
        return EntityBatch[int, Entity]().merge_list(
            [await entity_create.create_entity(id=index + 1) for index, entity_create in enumerate(self.entities)]
        )


@pytest.mark.asyncio
async def test_entity_get() -> None:
    """Test entity get."""
    entity = EntityGet(id=1)
    assert entity.id == 1


@pytest.mark.asyncio
async def test_entities_bulk_get() -> None:
    """Test entities bulk get."""
    batch = EntityBatch[int, Entity]().merge_list(
        [Entity(id=1, name="test"), Entity(id=2, name="test"), Entity(id=3, name="test")]
    )
    entities_bulk_get = await EntitiesBulkGet.from_batch(batch)
    assert entities_bulk_get.entities == [EntityGet(id=1), EntityGet(id=2), EntityGet(id=3)]


@pytest.mark.asyncio
async def test_entities_bulk_update() -> None:
    """Test entities bulk update."""
    batch = EntityBatch[int, Entity]().merge_list(
        [Entity(id=1, name="test"), Entity(id=2, name="test"), Entity(id=3, name="test")]
    )
    update_batch = EntitiesBulkUpdate(
        entities=[EntityUpdate(id=1, name="test2"), EntityUpdate(id=2, name="test2"), EntityUpdate(id=3, name="test2")]
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
    """Test entities bulk create."""
    create_batch = EntitiesBulkCreate(
        entities=[EntityCreate(name="test"), EntityCreate(name="test"), EntityCreate(name="test")]
    )
    created_batch = await create_batch.create_batch()
    assert created_batch.to_list() == [
        Entity(id=1, name="test"),
        Entity(id=2, name="test"),
        Entity(id=3, name="test"),
    ]
