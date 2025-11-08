"""Mapped batches."""

from typing import Self

from sqlalchemy import ScalarResult
from sqlalchemy.ext.asyncio import AsyncSession

from haolib.batches.batch import Batch
from haolib.batches.mapped import MappedBatch, UpdateableMappedBatch
from haolib.database.models.abstract.mapped import AbstractMappedModel, AbstractUpdateableMappedModel


class SQLAlchemyBatch[T_Key, T_Mapped: AbstractMappedModel](Batch[T_Key, T_Mapped]):
    """SQLAlchemy mapped batch."""

    def merge_from_scalars(self, scalars: ScalarResult[T_Mapped]) -> Self:
        """Merge the values from scalars to the batch.

        Args:
            scalars: The scalars to merge from.

        Returns:
            Self: The updated batch.

        """

        self.merge_list(list(scalars.all()))

        return self

    async def merge_to_db(self, session: AsyncSession) -> Self:
        """Merge the batch to the database.

        Args:
            session: The session to merge to.

        Returns:
            Self: The updated batch.

        """

        for mapped in self.to_list():
            await session.merge(mapped)

        return self


class SQLAlchemyMappedBatch[T_Key, T_Mapped: AbstractMappedModel, T_MappedTo](
    MappedBatch[T_Key, T_Mapped, T_MappedTo], SQLAlchemyBatch[T_Key, T_Mapped]
):
    """SQLAlchemy mapped batch."""


class SQLAlchemyUpdateableMappedBatch[T_Key, T_Mapped: AbstractUpdateableMappedModel, T_MappedTo](
    UpdateableMappedBatch[T_Key, T_Mapped, T_MappedTo], SQLAlchemyBatch[T_Key, T_Mapped]
):
    """SQLAlchemy updateable mapped batch."""
