"""SQLAlchemy config."""

from pydantic_settings import BaseSettings


class SQLAlchemyConfig(BaseSettings):
    """SQLAlchemy config.

    This config is used to configure the sqlalchemy.

    Attributes:
        url (str): The url of the sqlalchemy.

    """

    url: str
