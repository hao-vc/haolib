"""Mixins for models."""

from datetime import datetime
from uuid import UUID, uuid7  # type: ignore[attr-defined]

from sqlalchemy import DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import UUID as SAUUID


class SQLAlchemyIdModel[T_Id: UUID]:
    """ID model mixin.

    Adds an ID column to the model. It is a UUID column with a default value of randomly generated UUID.

    """

    id: Mapped[T_Id] = mapped_column(
        SAUUID,
        default=uuid7,
        primary_key=True,
    )


class SQLAlchemyDateTimeModel:
    """DateTime model mixin.

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
