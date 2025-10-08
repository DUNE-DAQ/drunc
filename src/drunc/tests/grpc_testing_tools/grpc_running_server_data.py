from typing import Any


class RunningGrpcServer:
    """
    Abstract representation of a Running gRPC Server Process
    The server could have been started via any supported method (multiprocessing, SSH, etc.)
    """

    def __init__(self, process_id: str, target_func: Any, args: tuple, kwargs: dict):
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
        self._process = None
        self._started = False
        self.startup_error = None
        self.host = None
        self.server_id = None
        self.port = None
        self.server_type = None
        self.ready_event = None
        self.stop_event = None

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
    def process(self) -> Any:
        """Get the underlying process object (implementation-specific)."""
        return self._process

    def set_process(self, process: Any) -> None:
        """Set the underlying process object."""
        self._process = process

    def mark_started(self) -> None:
        """Mark this process as started."""
        self._started = True

    def set_server_info(
        self, server_id: str, host: str, port: int, server_type: str
    ) -> None:
        """Set the server information for this process."""
        self.server_id = server_id
        self.host = host
        self.port = port
        self.server_type = server_type
