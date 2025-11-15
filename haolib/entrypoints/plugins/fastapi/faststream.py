"""FastAPI FastStream plugin."""

from dishka.integrations.faststream import setup_dishka as setup_dishka_faststream
from faststream._internal.broker import BrokerUsecase as BrokerType

from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin
from haolib.entrypoints.plugins.fastapi.dishka import FastAPIDishkaPlugin


class FastAPIFastStreamPlugin[T_BrokerType: BrokerType](AbstractEntrypointPlugin[FastAPIEntrypoint]):
    """Plugin for adding FastStream integration to FastAPI entrypoints.

    Example:
        ```python
        from fastapi import FastAPI
        from faststream.confluent.fastapi import KafkaRouter

        router = KafkaRouter()
        app = FastAPI()
        entrypoint = FastAPIEntrypoint(app=app).use_plugin(FastAPIFastStreamPlugin(router=router))
        ```

    """

    def __init__(
        self,
        broker: T_BrokerType,
    ) -> None:
        """Initialize the FastAPI FastStream plugin.

        Args:
            broker: The FastStream broker instance.

        """
        self._broker = broker

    def get_broker(self) -> T_BrokerType:
        """Get the FastStream broker instance.

        Returns:
            The FastStream broker instance.

        """
        return self._broker

    def apply(self, component: FastAPIEntrypoint) -> FastAPIEntrypoint:
        """Apply FastStream integration to the entrypoint.

        Args:
            component: The FastAPI entrypoint to configure.

        Returns:
            The configured entrypoint.

        """

        fastapi_dishka_plugin = component.plugin_registry.get_plugin(FastAPIDishkaPlugin)
        if fastapi_dishka_plugin is not None:
            setup_dishka_faststream(container=fastapi_dishka_plugin.get_container(), broker=self._broker)

        return component
