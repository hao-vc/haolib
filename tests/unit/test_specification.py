"""Test specification."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from sqlalchemy import ForeignKey, String, not_, or_, select
from sqlalchemy.orm import Mapped, mapped_column, relationship

from haolib.database.models.base.sqlalchemy import SQLAlchemyBaseModel
from haolib.database.specification.base import (
    BaseSpecification,
    EqualsSpecification,
    GreaterThanOrEqualsToSpecification,
    GreaterThanSpecification,
    ILikeSpecification,
    InListSpecification,
    LessThanOrEqualsToSpecification,
    LessThanSpecification,
    LikeSpecification,
    OrderBySpecification,
    SubListSpecification,
)
from haolib.database.specification.sqlalchemy import add_order_by_specifications_to_query, add_specifications_to_query
from haolib.enums.filter import OrderByType


class ObjectForTest:
    """Test object."""

    name: str | None
    age: int | None
    tags: list[str] | None
    is_active: bool | None
    created_at: datetime | None

    def __init__(
        self,
        *,
        name: str | None = None,
        age: int | None = None,
        tags: list[str] | None = None,
        is_active: bool | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self.name = name
        self.age = age
        self.tags = tags
        self.is_active = is_active
        self.created_at = created_at


def test_equals_specification() -> None:
    """Test equals specification."""
    assert EqualsSpecification("name", "John").is_satisfied_by(ObjectForTest(name="John"))
    assert (~EqualsSpecification("name", "John")).is_satisfied_by(ObjectForTest(name="Jane"))


def test_greater_than_specification() -> None:
    """Test greater than specification."""
    assert GreaterThanSpecification("age", 18).is_satisfied_by(ObjectForTest(age=25))
    assert (~GreaterThanSpecification("age", 18)).is_satisfied_by(ObjectForTest(age=15))


def test_less_than_specification() -> None:
    """Test less than specification."""
    assert LessThanSpecification("age", 18).is_satisfied_by(ObjectForTest(age=15))
    assert (~LessThanSpecification("age", 18)).is_satisfied_by(ObjectForTest(age=25))


def test_greater_than_or_equals_to_specification() -> None:
    """Test greater than or equals to specification."""
    assert GreaterThanOrEqualsToSpecification("age", 18).is_satisfied_by(ObjectForTest(age=25))
    assert GreaterThanOrEqualsToSpecification("age", 18).is_satisfied_by(ObjectForTest(age=18))
    assert (~GreaterThanOrEqualsToSpecification("age", 18)).is_satisfied_by(ObjectForTest(age=15))


def test_less_than_or_equals_to_specification() -> None:
    """Test less than or equals to specification."""
    assert LessThanOrEqualsToSpecification("age", 18).is_satisfied_by(ObjectForTest(age=17))
    assert LessThanOrEqualsToSpecification("age", 18).is_satisfied_by(ObjectForTest(age=18))
    assert (~LessThanOrEqualsToSpecification("age", 18)).is_satisfied_by(ObjectForTest(age=25))


def test_not_equals_specification() -> None:
    """Test not equals specification."""
    assert (~EqualsSpecification("name", "John")).is_satisfied_by(ObjectForTest(name="Jane"))


def test_not_in_list_specification() -> None:
    """Test not in list specification."""
    assert (~InListSpecification("name", ["John", "Jane"])).is_satisfied_by(ObjectForTest(name="Urna"))


def test_in_list_specification() -> None:
    """Test in list specification."""
    assert InListSpecification("name", ["John", "Jane"]).is_satisfied_by(ObjectForTest(name="Jane"))
    assert not InListSpecification("name", ["John", "Jane"]).is_satisfied_by(ObjectForTest(name="Urna"))


def test_sub_list_specification() -> None:
    """Test sub list specification."""
    assert SubListSpecification("tags", ["John"]).is_satisfied_by(ObjectForTest(tags=["Jane", "John"]))
    assert not SubListSpecification("tags", ["John"]).is_satisfied_by(ObjectForTest(tags=["Jane"]))


def test_not_sub_list_specification() -> None:
    """Test not sub list specification."""
    assert (~SubListSpecification("tags", ["Jane"])).is_satisfied_by(ObjectForTest(tags=["John"]))
    assert (~SubListSpecification("tags", ["John"])).is_satisfied_by(ObjectForTest(tags=["Johan"]))


def test_like_specification() -> None:
    """Test like specification."""
    assert LikeSpecification("name", "John").is_satisfied_by(ObjectForTest(name="John"))
    assert not LikeSpecification("name", "John").is_satisfied_by(ObjectForTest(name="Jane"))


def test_not_like_specification() -> None:
    """Test not like specification."""
    assert (~LikeSpecification("name", "John")).is_satisfied_by(ObjectForTest(name="Jane"))
    assert (~LikeSpecification("name", "John")).is_satisfied_by(ObjectForTest(name="Johan"))


def test_not_ilike_specification() -> None:
    """Test not ilike specification."""
    assert (~ILikeSpecification("name", "John")).is_satisfied_by(ObjectForTest(name="Jane"))
    assert (~ILikeSpecification("name", "John")).is_satisfied_by(ObjectForTest(name="Johan"))


def test_ilike_specification() -> None:
    """Test ilike specification."""
    assert ILikeSpecification("name", "John").is_satisfied_by(ObjectForTest(name="John"))
    assert not ILikeSpecification("name", "John").is_satisfied_by(ObjectForTest(name="Jane"))


class TagModel(SQLAlchemyBaseModel):
    """Test model."""

    __tablename__ = "tags"

    name: Mapped[str] = mapped_column(String, primary_key=True)
    object_name: Mapped[str] = mapped_column(String, ForeignKey("objects.name"))
    object: Mapped[ObjectForTestModel | None] = relationship("ObjectForTestModel", back_populates="tags")


class ObjectForTestModel(SQLAlchemyBaseModel):
    """Test model."""

    __tablename__ = "objects"

    name: Mapped[str | None] = mapped_column(String, primary_key=True)
    age: Mapped[int | None]
    tags: Mapped[list[TagModel] | None] = relationship("TagModel", back_populates="object")
    is_active: Mapped[bool | None]
    created_at: Mapped[datetime | None]


def test_sqlalchemy_specification_equals() -> None:
    """Test sqlalchemy specification."""

    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [EqualsSpecification("name", "John")]
        )
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.name == "John"))
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [~EqualsSpecification("name", "John")]
        )
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.name != "John"))


def test_sqlalchemy_specification_greater_than() -> None:
    """Test greater than specification."""
    age_to_compare = 18
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [GreaterThanSpecification("age", age_to_compare)]
        )
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.age > age_to_compare))
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [~GreaterThanSpecification("age", age_to_compare)]
        )
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.age <= age_to_compare))


def test_sqlalchemy_specification_less_than() -> None:
    """Test less than specification."""
    age_to_compare = 18
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [LessThanSpecification("age", age_to_compare)]
        )
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.age < age_to_compare))
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [~LessThanSpecification("age", age_to_compare)]
        )
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.age >= age_to_compare))


def test_sqlalchemy_specification_greater_than_or_equals_to() -> None:
    """Test greater than or equals to specification."""
    age_to_compare = 18
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [GreaterThanOrEqualsToSpecification("age", age_to_compare)]
        )
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.age >= age_to_compare))
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [~GreaterThanOrEqualsToSpecification("age", age_to_compare)]
        )
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.age < age_to_compare))


def test_sqlalchemy_specification_less_than_or_equals_to() -> None:
    """Test less than or equals to specification."""
    age_to_compare = 18
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [LessThanOrEqualsToSpecification("age", age_to_compare)]
        )
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.age <= age_to_compare))
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [~LessThanOrEqualsToSpecification("age", age_to_compare)]
        )
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.age > age_to_compare))


def test_sqlalchemy_specification_like() -> None:
    """Test like specification."""
    assert str(
        add_specifications_to_query(select(ObjectForTestModel), ObjectForTestModel, [LikeSpecification("name", "John")])
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.name.like("John")))
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [~LikeSpecification("name", "John")]
        )
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.name.not_like("John")))


def test_sqlalchemy_specification_ilike() -> None:
    """Test ilike specification."""
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [ILikeSpecification("name", "John")]
        )
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.name.ilike("John")))
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [~ILikeSpecification("name", "John")]
        )
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.name.not_ilike("John")))


def test_sqlalchemy_specification_in_list() -> None:
    """Test in list specification."""
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [InListSpecification("name", ["John", "Jane"])]
        )
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.name.in_(["John", "Jane"])))
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [~InListSpecification("name", ["John", "Jane"])]
        )
    ) == str(select(ObjectForTestModel).where(ObjectForTestModel.name.not_in(["John", "Jane"])))


def test_sqlalchemy_specification_sub_list() -> None:
    """Test sub list specification."""
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [SubListSpecification("name", ["John", "Jane"])]
        )
    ) == str(select(ObjectForTestModel).where(or_(*[ObjectForTestModel.name == value for value in ["John", "Jane"]])))
    assert str(
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [~SubListSpecification("name", ["John", "Jane"])]
        )
    ) == str(
        select(ObjectForTestModel).where(not_(or_(*[ObjectForTestModel.name == value for value in ["John", "Jane"]])))
    )


def test_sqlalchemy_specification_order_by() -> None:
    """Test order by specification."""
    assert str(
        add_order_by_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [OrderBySpecification("name", OrderByType.ASC)]
        )
    ) == str(select(ObjectForTestModel).order_by(ObjectForTestModel.name.asc()))
    assert str(
        add_order_by_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [OrderBySpecification("name", OrderByType.DESC)]
        )
    ) == str(select(ObjectForTestModel).order_by(ObjectForTestModel.name.desc()))


class BaseSpecificationForTest(BaseSpecification):
    """Test specification."""

    def is_satisfied_by(self, obj: Any) -> bool:
        """Test specification."""
        return bool(obj)


def test_sqlalchemy_specification_raises_error_when_specification_is_not_valid() -> None:
    """Test raises error when specification is not valid."""
    with pytest.raises(ValueError):
        add_specifications_to_query(
            select(ObjectForTestModel), ObjectForTestModel, [BaseSpecificationForTest("name", "John")]
        )
