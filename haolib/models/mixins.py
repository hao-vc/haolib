"""Mixins for models."""

from collections.abc import Callable
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import DateTime, func
from sqlalchemy.orm import Mapped, mapped_column

from haolib.utils.uuidv7 import get_random_uuid


def get_custom_id_mixin(
    get_uuid_function: Callable[[], UUID] | None = None,
) -> Any:
    """Get a custom id mixin."""
    return type(
        "IdMixin",
        (ModelWithIdMixin,),
        {
            "id": mapped_column(default=get_uuid_function, primary_key=True)
            if get_uuid_function
            else mapped_column(default=get_random_uuid, primary_key=True),
        },
    )


class ModelWithIdMixin:
    """Id mixin.

    Adds an id column to the model. It is a UUID column with a default value of randomly generated UUID.
    """

    id: Mapped[UUID] = mapped_column(
        default=get_random_uuid,
        primary_key=True,
    )


def get_custom_date_time_mixin(
    created_at_default: Callable[[], datetime] | None = None,
    updated_at_default: Callable[[], datetime] | None = None,
) -> Any:
    """Get a custom date time mixin."""
    return type(
        "DateTimeMixin",
        (ModelWithDateTimeMixin,),
        {
            "created_at": mapped_column(
                DateTime(timezone=True),
                default=created_at_default,
            )
            if created_at_default
            else mapped_column(DateTime(timezone=True), server_default=func.now()),
            "updated_at": mapped_column(
                DateTime(timezone=True),
                default=updated_at_default,
                onupdate=func.now(),
            )
            if updated_at_default
            else mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
        },
    )


class ModelWithDateTimeMixin:
    """DateTime mixin.

    Adds created_at and updated_at columns to the model. It is a DateTime column with a default value of func.now.
    """

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


def get_entity_mixin(
    get_uuid_function: Callable[[], UUID] | None = None,
    created_at_default: Callable[[], datetime] | None = None,
    updated_at_default: Callable[[], datetime] | None = None,
) -> Any:
    """Get a entity mixin."""
    return type(
        "EntityModelMixin",
        (get_custom_id_mixin(get_uuid_function), get_custom_date_time_mixin(created_at_default, updated_at_default)),
        {},
    )


class EntityModelMixin(ModelWithIdMixin, ModelWithDateTimeMixin):
    """Entity mixin.

    Adds id and created_at and updated_at columns to the model.
    """
