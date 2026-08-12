import multiprocessing

from drunc.grpc_testing_tools.available_grpc_servers import ServerType
from drunc.grpc_testing_tools.stubs import P, TargetFunc


class RunningGrpcServer:
    """
    Abstract representation of a Running gRPC Server Process
    The server could have been started via any supported method (multiprocessing, SSH, etc.)
    """

    def __init__(
        self,
        process_id: str,
        target_func: TargetFunc[P, object],
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
        self._process: multiprocessing.Process | None = None
        self._started = False
        self.startup_error: Exception | None = None
        self.host: str | None = None
        self.server_id: str | None = None
        self.port: int | None = None
        self.server_type: str | ServerType | None = None
        self.ready_event: multiprocessing.synchronize.Event | None = None
        self.stop_event: multiprocessing.synchronize.Event | None = None

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
    def process(self) -> multiprocessing.Process | None:
        """Get the underlying process object (implementation-specific)."""
        return self._process

    def set_process(self, process: multiprocessing.Process) -> None:
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
