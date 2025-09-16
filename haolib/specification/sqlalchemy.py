"""Module containing utilities."""

from typing import Any

from sqlalchemy import Select, not_, or_
from sqlalchemy.orm import DeclarativeBase, InstrumentedAttribute

from haolib.enums.filter import OrderByType
from haolib.specification.base import (
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
from haolib.utils.rattrs import rgetattr


# This noqa is here since this function is not really that complex
def add_specifications_to_query[SelectType: Any](  # noqa: C901 PLR0912
    query: Select[SelectType],
    table: type[DeclarativeBase],
    specifications: list[BaseSpecification],
) -> Select[SelectType]:
    """Add specifications to a query.

    Args:
        query (Select[SelectType]): The query to add specifications to.
        table (type[DeclarativeBase]): The table to filter.
        specifications (list[BaseSpecification]): The specifications.

    Returns:
        Select[SelectType]: The result query.

    """
    for specification in specifications:
        table_column_obj: InstrumentedAttribute = rgetattr(table, specification.field)

        match specification:
            case EqualsSpecification(is_inverted=False):
                query = query.where(table_column_obj == specification.value)
            case EqualsSpecification(is_inverted=True):
                query = query.where(table_column_obj != specification.value)
            case InListSpecification(is_inverted=False):
                query = query.where(table_column_obj.in_(specification.value))
            case InListSpecification(is_inverted=True):
                query = query.where(table_column_obj.not_in(specification.value))
            case SubListSpecification(is_inverted=False):
                query = query.where(or_(*[table_column_obj == value for value in specification.value]))
            case SubListSpecification(is_inverted=True):
                query = query.where(not_(or_(*[table_column_obj == value for value in specification.value])))
            case GreaterThanSpecification(is_inverted=False):
                query = query.where(table_column_obj > specification.value)
            case GreaterThanSpecification(is_inverted=True):
                query = query.where(table_column_obj <= specification.value)
            case GreaterThanOrEqualsToSpecification(is_inverted=False):
                query = query.where(table_column_obj >= specification.value)
            case GreaterThanOrEqualsToSpecification(is_inverted=True):
                query = query.where(table_column_obj < specification.value)
            case LessThanSpecification(is_inverted=False):
                query = query.where(table_column_obj < specification.value)
            case LessThanSpecification(is_inverted=True):
                query = query.where(table_column_obj >= specification.value)
            case LessThanOrEqualsToSpecification(is_inverted=False):
                query = query.where(table_column_obj <= specification.value)
            case LessThanOrEqualsToSpecification(is_inverted=True):
                query = query.where(table_column_obj > specification.value)
            case LikeSpecification(is_inverted=False):
                query = query.where(table_column_obj.like(specification.value))
            case LikeSpecification(is_inverted=True):
                query = query.where(table_column_obj.not_like(specification.value))
            case ILikeSpecification(is_inverted=False):
                query = query.where(table_column_obj.ilike(specification.value))
            case ILikeSpecification(is_inverted=True):
                query = query.where(table_column_obj.not_ilike(specification.value))
            case _:
                raise ValueError("Incorrect specification passed.")

    return query


def add_order_by_specifications_to_query[SelectType: Any](
    query: Select[SelectType],
    table: type[DeclarativeBase],
    order_by_specifications: list[OrderBySpecification],
) -> Select[SelectType]:
    """Add order by to a query.

    Args:
        query (Select[SelectType]): The query to add order by to.
        table (type[DeclarativeBase]): The table to order by.
        order_by_specifications (list[OrderBySpecification]): The order by specifications.

    Returns:
        Select[SelectType]: The result query.

    """
    for order_by_specification in order_by_specifications:
        table_column_obj: InstrumentedAttribute = rgetattr(table, order_by_specification.field)
        query = query.order_by(
            table_column_obj.asc() if order_by_specification.type == OrderByType.ASC else table_column_obj.desc(),
        )

    return query
