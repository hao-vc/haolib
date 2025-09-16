"""Server config."""

from pydantic import Field
from pydantic_settings import BaseSettings


class ServerConfig(BaseSettings):
    """Server config.

    This config is used to configure the server.

    Attributes:
        host (str): The host of the server. Defaults to "localhost".
        port (int): The port of the server. Defaults to 8000.

    """

    host: str = Field(default="localhost")
    port: int = Field(default=8000)
