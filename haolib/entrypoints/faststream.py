"""FastStream entrypoint."""

from typing import TYPE_CHECKING, Any, Self

from dishka.integrations.faststream import setup_dishka
from faststream._internal.broker import BrokerUsecase as BrokerType

from haolib.entrypoints.base import Entrypoint
from haolib.entrypoints.exceptions import EntrypointsInconsistencyError

if TYPE_CHECKING:
    from dishka import AsyncContainer
    from faststream import FastStream
    from faststream._internal.broker import BrokerUsecase as BrokerType


class FastStreamEntrypoint(Entrypoint):
    """FastStream entrypoint."""

    def __init__(
        self,
        broker: BrokerType[Any, Any] | None = None,
        app: FastStream | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize the FastStream entrypoint.

        Args:
            broker: The FastStream broker.
            app: The FastStream app.
            *args: The arguments to pass to the FastStream run method.
            **kwargs: The keyword arguments to pass to the FastStream run method.

        """

        if broker is None and app is None:
            raise EntrypointsInconsistencyError("Either broker or app must be provided.")

        if broker is not None and app is not None:
            raise EntrypointsInconsistencyError("Only one of broker or app must be provided.")

        self._run_args = args
        self._run_kwargs = kwargs

        self._broker: BrokerType[Any, Any] | None = broker
        self._app: FastStream | None = app

    def setup_dishka(self, container: AsyncContainer) -> Self:
        """Setup dishka."""

        setup_dishka(container=container, app=self._app, broker=self._broker, finalize_container=False)

        return self

    def get_broker(self) -> BrokerType[Any, Any]:
        """Get the broker."""
        if self._broker is None:
            raise EntrypointsInconsistencyError("Broker is not set.")

        return self._broker

    def get_app(self) -> FastStream:
        """Get the FastStream app."""
        if self._app is None:
            raise EntrypointsInconsistencyError("App is not set.")

        return self._app

    async def run(self) -> None:
        """Run the FastStream entrypoint.

        Args:
            *args: The arguments to pass to the FastStream run method.
            **kwargs: The keyword arguments to pass to the FastStream run method.

        """
        if self._app is None:
            raise EntrypointsInconsistencyError("App is not set.")

        await self._app.run(*self._run_args, **self._run_kwargs)
