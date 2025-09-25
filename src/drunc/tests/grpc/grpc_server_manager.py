"""
Abstract gRPC Server Manager

Provides abstraction over gRPC server lifecycle management,
separating server-specific configuration and startup logic from the underlying
process execution mechanism (multiprocessing, SSH, ...).
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from drunc.tests.grpc.available_grpc_servers import ServerType
from drunc.tests.grpc.process_connection_manager import (
    ProcessConnectionManager,
    RunningGrpcServer,
)


class GrpcServerConfig:
    """
    Configuration container for a gRPC server instance.

    Encapsulates all parameters needed to start and manage a gRPC server,
    providing a consistent interface across different execution environments.
    """

    def __init__(
        self,
        server_id: str,
        server_type: ServerType,
        host: str,
        port: int,
        max_workers: int,
        log_file: str,
        server_options: List[Tuple[str, Any]] = None,
        client_options: List[Tuple[str, Any]] = None,
        **kwargs,
    ):
        """
        Initialise gRPC server configuration.

        Args:
            server_id: Unique identifier for this server instance
            server_type: Type of server ('manager', 'root_controller', 'child_controller')
            host: Hostname or IP address where the server will run
            port: TCP port for the server to bind to
            max_workers: Maximum number of worker threads for the server
            log_file: Path to log file for server output
            server_options: gRPC server configuration options
            client_options: gRPC client configuration options (for servers that act as clients)
            **kwargs: Additional server-specific parameters
        """
        self.server_id = server_id
        self.server_type = server_type
        self.host = host
        self.port = port
        self.max_workers = max_workers
        self.log_file = log_file
        self.server_options = server_options or []
        self.client_options = client_options or []

        if server_type == ServerType.MANAGER:
            required_params = []
        elif server_type == ServerType.ROOT_CONTROLLER:
            required_params = ["manager_port"]
        elif server_type == ServerType.CHILD_CONTROLLER:
            required_params = ["root_port", "child_name"]
        else:
            required_params = []

        self.extra_params = {}

        for key, param in kwargs.items():
            if key in required_params:
                self.extra_params[key] = param
                required_params.remove(key)
            else:
                print(
                    f"Warning: Unrecognized parameter '{key}' for server type '{server_type}'"
                )

        for param in required_params:
            raise ValueError(
                f"Missing required parameter '{param}' for server type '{server_type}'"
            )

    def get_param(self, key: str, default: Any = None) -> Any:
        """Get an additional parameter value."""
        return self.extra_params.get(key, default)


class GrpcServerManager(ABC):
    """
    Abstract base class for managing gRPC server lifecycle.

    Defines the interface that all gRPC server management implementations must
    follow, whether using local multiprocessing or remote SSH execution.
    """

    def __init__(self, connection_manager: ProcessConnectionManager):
        """
        Initialise gRPC server manager.

        Args:
            connection_manager: Process execution manager (multiprocessing or SSH)
        """
        self.connection_manager = connection_manager
        self.server_handles: Dict[str, RunningGrpcServer] = {}

    @abstractmethod
    def start_manager_server(self, config: GrpcServerConfig) -> RunningGrpcServer:
        """
        Start a Manager gRPC server.

        Args:
            config: Configuration for the Manager server

        Returns:
            ProcessHandle for the started server

        Raises:
            RuntimeError: If server cannot be started
        """
        pass

    @abstractmethod
    def start_root_controller_server(
        self, config: GrpcServerConfig
    ) -> RunningGrpcServer:
        """
        Start a RootController gRPC server.

        Args:
            config: Configuration for the RootController server

        Returns:
            ProcessHandle for the started server

        Raises:
            RuntimeError: If server cannot be started
        """
        pass

    @abstractmethod
    def start_child_controller_server(
        self, config: GrpcServerConfig
    ) -> RunningGrpcServer:
        """
        Start a ChildController gRPC server.

        Args:
            config: Configuration for the ChildController server

        Returns:
            ProcessHandle for the started server

        Raises:
            RuntimeError: If server cannot be started
        """
        pass

    @abstractmethod
    def wait_for_server_ready(self, server_id: str, timeout: float = 10.0) -> bool:
        """
        Wait for a server to signal that it's ready to accept connections.

        Args:
            server_id: ID of the server to wait for
            timeout: Maximum time to wait in seconds

        Returns:
            True if server became ready, False if timeout occurred
        """
        pass

    @abstractmethod
    def stop_server(self, server_id: str, timeout: float = 10.0) -> None:
        """
        Stop a running gRPC server gracefully.

        Args:
            server_id: ID of the server to stop
            timeout: Maximum time to wait for graceful shutdown

        Raises:
            RuntimeError: If server cannot be stopped
        """
        pass

    @abstractmethod
    def stop_all_servers(self, timeout: float = 10.0) -> None:
        """
        Stop all managed gRPC servers.

        Args:
            timeout: Maximum time to wait for each server to stop
        """
        pass

    @abstractmethod
    def get_server_handle(self, server_id: str) -> Optional[RunningGrpcServer]:
        """
        Get process handle for a managed server.

        Args:
            server_id: ID of the server

        Returns:
            ProcessHandle if server exists, None otherwise
        """
        pass

    def get_server_handles(self) -> dict[str, RunningGrpcServer]:
        return self.server_handles

    def is_server_running(self, server_id: str) -> bool:
        """
        Check if a gRPC server is currently running using channel state checking.

        Args:
            server_id: ID of the server to check

        Returns:
            True if server is running and channel is ready, False otherwise
        """
        if self.get_server_handles() is None:
            return False

        server_handle = self.get_server_handles().get(server_id)
        if server_handle is None or not server_handle.is_valid():
            return False

        channel = None

        try:
            import grpc

            channel = grpc.insecure_channel(
                f"{server_handle.host}:{server_handle.port}",
            )
            grpc.channel_ready_future(channel).result(timeout=2.0)

        except Exception:
            return False
        finally:
            if channel is not None:
                channel.close()

    @abstractmethod
    def cleanup(self) -> None:
        """Clean up all resources and stop remaining servers."""
        pass
