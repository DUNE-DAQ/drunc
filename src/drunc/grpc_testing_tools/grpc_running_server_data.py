from typing import Protocol

from drunc.grpc_testing_tools.available_grpc_servers import ServerType


class TargetFunc(Protocol):
    """Callable target used to launch a server process."""

    def __call__(self, *args: object, **kwargs: object) -> object: ...


class ProcessLike(Protocol):
    """Minimal process API required by connection managers."""

    def start(self) -> object: ...

    def is_alive(self) -> bool: ...

    def terminate(self) -> object: ...

    def kill(self) -> object: ...

    def join(self, timeout: float | None = None) -> object: ...


class EventLike(Protocol):
    """Minimal event API used for readiness/stop coordination."""

    def set(self) -> object: ...

    def is_set(self) -> bool: ...


class RunningGrpcServer:
    """
    Abstract representation of a Running gRPC Server Process
    The server could have been started via any supported method (multiprocessing, SSH, etc.)
    """

    def __init__(
        self,
        process_id: str,
        target_func: TargetFunc,
        args: tuple[object, ...],
        kwargs: dict[str, object],
    ) -> None:
        """
        Initialise process handle with execution parameters.

        Args:
            process_id: Unique identifier for this process
            target_func: Function to execute as the process
            args: Positional arguments for the target function
            kwargs: Keyword arguments for the target function
        """
        self.process_id = process_id
        self.target_func = target_func
        self.args = args
        self.kwargs = kwargs
        self._process: ProcessLike | None = None
        self._started = False
        self.startup_error: Exception | None = None
        self.host: str | None = None
        self.server_id: str | None = None
        self.port: int | None = None
        self.server_type: str | ServerType | None = None
        self.ready_event: EventLike | None = None
        self.stop_event: EventLike | None = None

    def is_valid(self) -> bool:
        """Check if the process handle is valid"""
        required_not_none = [
            self.process_id,
            self.host,
            self.server_id,
            self.port,
            self.server_type,
        ]
        return all(param is not None for param in required_not_none)

    @property
    def started(self) -> bool:
        """Check if process has been started."""
        return self._started

    @property
    def process(self) -> ProcessLike | None:
        """Get the underlying process object (implementation-specific)."""
        return self._process

    def set_process(self, process: ProcessLike) -> None:
        """Set the underlying process object."""
        self._process = process

    def mark_started(self) -> None:
        """Mark this process as started."""
        self._started = True

    def set_server_info(
        self, server_id: str, host: str, port: int, server_type: str | ServerType
    ) -> None:
        """Set the server information for this process."""
        self.server_id = server_id
        self.host = host
        self.port = port
        self.server_type = server_type
