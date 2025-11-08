"""SQLAlchemy providers."""

from collections.abc import AsyncGenerator

from dishka import Provider, Scope, provide
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from haolib.configs.sqlalchemy import SQLAlchemyConfig
from haolib.database.transactions.sqlalchemy import SQLAlchemyTransaction


class SQLAlchemyProvider(Provider):
    """SQLAlchemy provider."""

    @provide(scope=Scope.APP)
    async def db_engine(self, sqlalchemy_config: SQLAlchemyConfig) -> AsyncEngine:
        """Get db engine."""
        if sqlalchemy_config.use_pool:
            return create_async_engine(sqlalchemy_config.url)

        return create_async_engine(sqlalchemy_config.url, poolclass=NullPool)

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
            yield session

    @provide(scope=Scope.REQUEST)
    async def transaction(
        self,
        new_session: AsyncSession,
    ) -> AsyncGenerator[SQLAlchemyTransaction]:
        """Get a new transaction.

        Args:
            new_session (AsyncGenerator[AsyncSession]): The new session.

        Returns:
            SQLAlchemyTransaction: A new transaction.

        """
        async with SQLAlchemyTransaction(new_session) as transaction:
            yield transaction
