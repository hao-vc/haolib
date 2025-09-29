"""SQLAlchemy providers."""

from collections.abc import AsyncGenerator

from dishka import Provider, Scope, provide
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from haolib.configs.sqlalchemy import SQLAlchemyConfig


class SQLAlchemyProvider(Provider):
    """SQLAlchemy provider."""

    @provide(scope=Scope.APP)
    async def db_engine(self, sqlalchemy_config: SQLAlchemyConfig) -> AsyncEngine:
        """Get db engine."""
        return create_async_engine(sqlalchemy_config.url)

    @provide(scope=Scope.APP)
    async def db_session_maker(self, db_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
        """Get db session maker.

        Args:
            db_engine (AsyncEngine): The db engine.

        Returns:
            async_sessionmaker[AsyncSession]: The db session maker.

        """
        return async_sessionmaker(db_engine, expire_on_commit=False)

    @provide(scope=Scope.REQUEST)
    async def new_session(
        self,
        db_session_maker: async_sessionmaker[AsyncSession],
    ) -> AsyncGenerator[AsyncSession]:
        """Get new database session.

        Args:
            db_session_maker (async_sessionmaker[AsyncSession]): The db session maker.

        Returns:
            AsyncSession: A new AsyncSessoin instance.

        """
        async with db_session_maker() as session:
            try:
                yield session
            except SQLAlchemyError:
                await session.rollback()
                raise
