"""SQLAlchemy config."""

from pydantic import Field
from pydantic_settings import BaseSettings


class SQLAlchemyConfig(BaseSettings):
    """SQLAlchemy config.

    This config is used to configure the sqlalchemy.

    Attributes:
        url (str): The url of the sqlalchemy.
        use_pool (bool): Whether to use a connection pool. Defaults to False.

    """

    url: str
    use_pool: bool = Field(default=False)
