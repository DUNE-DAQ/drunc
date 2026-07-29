"""
gRPC Server Manager

Provides unified server lifecycle management across different process execution
methods (multiprocessing, SSH, etc.). Delegates process creation and execution
to connection managers while handling server-specific configuration and coordination.
"""

import time
from typing import Dict, Optional, cast

from grpc import (
    FutureCancelledError,
    FutureTimeoutError,
    channel_ready_future,
    insecure_channel,
)

from drunc.grpc_testing_tools.grpc_running_server_data import (
    RunningGrpcServer,
    TargetFunc,
)
from drunc.grpc_testing_tools.grpc_server_config import GrpcServerConfig
from drunc.grpc_testing_tools.process_connection_manager import (
    ProcessConnectionManager,
)
from drunc.grpc_testing_tools.run_grpc_services import (
    run_child_controller_server,
    run_process_manager_server,
    run_root_controller_server,
)
from drunc.process_manager.configuration import (
    ProcessManagerTypes,
)


class GrpcServerManager:
    """
    Manager for gRPC server lifecycle across different execution environments.

    Provides unified interface for starting, stopping, and monitoring gRPC servers
    regardless of underlying process execution method (multiprocessing, SSH, etc.).
    Delegates actual process creation and management to connection managers while
    providing server-specific configuration and function references.
    """

    def __init__(self, connection_manager: ProcessConnectionManager):
        """
        Initialise gRPC server manager.

        Args:
            connection_manager: Process execution manager (e.g., multiprocessing or SSH)
        """
        self.connection_manager = connection_manager
        self.server_handles: Dict[str, RunningGrpcServer] = {}

    def start_manager_server(
        self,
        config: GrpcServerConfig,
        lifetime_manager_type: ProcessManagerTypes = ProcessManagerTypes.SSH_SHELL,
    ) -> RunningGrpcServer:
        """
        Start a Manager gRPC server.

        Creates and starts a Manager server process using the configured connection
        manager. The Manager is the top-level coordinator in the system hierarchy.

        Args:
            config: Configuration for the Manager server

        Returns:
            RunningGrpcServer handle for the started server

        Raises:
            RuntimeError: If server cannot be started
        """
        try:
            # Create process handle with complete server function and arguments
            handle = self.connection_manager.create_process(
                config.server_id,
                cast(TargetFunc, run_process_manager_server),
                config.max_workers,
                config.port,
                config.log_file,
                config.server_options,
                lifetime_manager_type=lifetime_manager_type,
            )

            # Set server information for tracking and connectivity checking
            handle.set_server_info(
                config.server_id, config.host, config.port, config.server_type
            )

            # Track server before starting
            self.server_handles[config.server_id] = handle

            # Start the process (connection manager handles implementation details)
            self.connection_manager.start_process(handle)

            return handle

        except Exception as e:
            # Clean up tracking on failure
            if config.server_id in self.server_handles:
                del self.server_handles[config.server_id]
            raise RuntimeError(
                f"Failed to start Manager server {config.server_id}: {e}"
            )

    def start_root_controller_server(
        self, config: GrpcServerConfig
    ) -> RunningGrpcServer:
        """
        Start a RootController gRPC server.

        Creates and starts a RootController server process. The RootController
        acts as an intermediate coordinator between the Manager and ChildControllers.

        Args:
            config: Configuration for the RootController server (must include manager_port)

        Returns:
            RunningGrpcServer handle for the started server

        Raises:
            RuntimeError: If server cannot be started
        """
        manager_port = config.get_param("manager_port")
        if manager_port is None:
            raise ValueError("RootController server requires 'manager_port' parameter")

        try:
            # Create process handle with complete server function and arguments
            handle = self.connection_manager.create_process(
                config.server_id,
                cast(TargetFunc, run_root_controller_server),
                config.max_workers,
                config.port,
                manager_port,
                config.log_file,
                config.server_options,
                config.client_options,
            )

            # Set server information
            handle.set_server_info(
                config.server_id, config.host, config.port, config.server_type
            )

            # Track server before starting
            self.server_handles[config.server_id] = handle

            # Start the process
            self.connection_manager.start_process(handle)

            return handle

        except Exception as e:
            # Clean up tracking on failure
            if config.server_id in self.server_handles:
                del self.server_handles[config.server_id]
            raise RuntimeError(
                f"Failed to start RootController server {config.server_id}: {e}"
            )

    def start_child_controller_server(
        self, config: GrpcServerConfig
    ) -> RunningGrpcServer:
        """
        Start a ChildController gRPC server.

        Creates and starts a ChildController server process. ChildControllers
        are leaf nodes that handle specific tasks and report to the RootController.

        Args:
            config: Configuration for the ChildController server (must include root_port and child_name)

        Returns:
            RunningGrpcServer handle for the started server

        Raises:
            RuntimeError: If server cannot be started
        """
        root_port = config.get_param("root_port")
        child_name = config.get_param("child_name")

        if root_port is None:
            raise ValueError("ChildController server requires 'root_port' parameter")
        if child_name is None:
            raise ValueError("ChildController server requires 'child_name' parameter")

        try:
            # Create process handle with complete server function and arguments
            handle = self.connection_manager.create_process(
                config.server_id,
                cast(TargetFunc, run_child_controller_server),
                config.max_workers,
                config.port,
                root_port,
                child_name,
                config.log_file,
                config.server_options,
                config.client_options,
            )

            # Set server information
            handle.set_server_info(
                config.server_id, config.host, config.port, config.server_type
            )

            # Track server before starting
            self.server_handles[config.server_id] = handle

            # Start the process
            self.connection_manager.start_process(handle)

            return handle

        except Exception as e:
            # Clean up tracking on failure
            if config.server_id in self.server_handles:
                del self.server_handles[config.server_id]
            raise RuntimeError(
                f"Failed to start ChildController server {config.server_id}: {e}"
            )

    def wait_for_server_ready(self, server_id: str, timeout: float = 10.0) -> bool:
        """
        Wait for a server to become ready and accept connections.

        Polls the server's gRPC port to determine when it's ready to handle requests.
        This method is implementation-agnostic and works regardless of how the
        server process was started.

        Args:
            server_id: ID of the server to wait for
            timeout: Maximum time to wait in seconds

        Returns:
            True if server is ready and accepting connections, False if timeout occurs
        """
        if server_id not in self.server_handles:
            print(f"Server {server_id} not found in server handles")
            return False

        start_time = time.time()

        while (time.time() - start_time) < timeout:
            if self.is_server_running(server_id):
                return True
            elapsed = time.time() - start_time
            print(f"Waiting for server {server_id} (elapsed: {elapsed:.1f}s)")
            time.sleep(0.5)

        print(f"Timed out waiting for server {server_id} to be accessible")
        return False

    def stop_server(self, server_id: str, timeout: float = 10.0) -> None:
        """
        Stop a running gRPC server gracefully.

        Delegates to the connection manager for actual process termination,
        handling any implementation-specific shutdown procedures (e.g., signalling
        stop events for multiprocessing, terminating SSH connections).

        Args:
            server_id: ID of the server to stop
            timeout: Maximum time to wait for graceful shutdown

        Raises:
            RuntimeError: If server cannot be stopped
        """
        if server_id not in self.server_handles:
            return

        handle = self.server_handles[server_id]

        # Delegate process stopping to connection manager
        self.connection_manager.stop_process(handle, timeout=timeout)

        # Remove from tracking
        del self.server_handles[server_id]

    def stop_all_servers(self, timeout: float = 10.0) -> None:
        """
        Stop all managed gRPC servers.

        Iterates through all tracked servers and stops them gracefully.
        Continues attempting to stop remaining servers even if individual
        stops fail.

        Args:
            timeout: Maximum time to wait for each server to stop
        """
        for server_id in list(self.server_handles.keys()):
            try:
                self.stop_server(server_id, timeout=timeout)
            except Exception as e:
                print(f"Warning: Error stopping server {server_id}: {e}")

    def get_server_handle(self, server_id: str) -> Optional[RunningGrpcServer]:
        """
        Get process handle for a managed server.

        Args:
            server_id: ID of the server

        Returns:
            RunningGrpcServer handle if server exists, None otherwise
        """
        return self.server_handles.get(server_id)

    def get_server_handles(self) -> Dict[str, RunningGrpcServer]:
        """
        Get all server handles managed by this instance.

        Returns:
            Dictionary mapping server IDs to their RunningGrpcServer handles
        """
        return self.server_handles

    def is_server_running(self, server_id: str) -> bool:
        """
        Check if a gRPC server is currently running and accepting connections.

        Uses gRPC channel readiness checking to determine if the server is
        operational. This is implementation-agnostic and works regardless of
        how the server process was started.

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

        channel = insecure_channel(
            f"{server_handle.host}:{server_handle.port}",
        )
        ready_future = channel_ready_future(channel)

        try:
            # Wait for channel to be ready (throws exception if unavailable)
            ready_future.result(timeout=1.0)
            channel.close()
            return True
        except FutureTimeoutError:
            channel.close()
            return False
        except FutureCancelledError:
            channel.close()
            return False

    def cleanup(self) -> None:
        """
        Clean up all resources and stop remaining servers.

        Stops all managed servers and delegates cleanup to the connection manager
        for any implementation-specific resource cleanup.
        """
        self.stop_all_servers()
        self.connection_manager.cleanup()
