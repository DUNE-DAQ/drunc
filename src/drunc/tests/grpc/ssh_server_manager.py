"""
SSH-based server manager for coordinating server lifecycle.

This module provides simple server lifecycle coordination using
an SSH connection manager with pre-built boot commands.
"""

from typing import Optional

from drunc.tests.grpc.grpc_server_manager import GrpcServerConfig, GrpcServerManager
from drunc.tests.grpc.process_connection_manager import RunningGrpcServer
from drunc.tests.grpc.ssh_connection_manager import SSHConnectionManager


class SSHGrpcServerManager(GrpcServerManager):
    """
    gRPC server manager using SSH for remote execution.

    Coordinates server lifecycle by delegating to SSH connection manager
    with pre-built boot commands. Does not handle command construction.
    """

    def __init__(self, connection_manager: SSHConnectionManager):
        """
        Initialise SSH gRPC server manager.

        Args:
            connection_manager: SSH connection manager with pre-built boot commands
        """
        super().__init__(connection_manager)

        # Track server handles for lifecycle management
        self.server_handles = {}

        print("SSH server manager initialised")

    def start_manager_server(self, config: GrpcServerConfig) -> RunningGrpcServer:
        """
        Start Manager server using pre-built boot command.

        Args:
            config: Manager server configuration (only server_id is used)

        Returns:
            ProcessHandle for the remote Manager server process

        Raises:
            RuntimeError: If server cannot be started
        """
        try:
            # Create process handle
            handle = self.connection_manager.create_process(
                config.server_id,
                lambda: None,  # Placeholder function for SSH execution
            )

            handle.set_server_info(
                config.server_id, config.host, config.port, config.server_type
            )

            # Track server handle before starting
            self.server_handles[config.server_id] = handle

            # Start the process using the standard start_process method
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
        Start RootController server using pre-built boot command.

        Args:
            config: RootController server configuration (only server_id is used)

        Returns:
            ProcessHandle for the remote RootController server process

        Raises:
            RuntimeError: If server cannot be started
        """
        try:
            # Create process handle
            handle = self.connection_manager.create_process(
                config.server_id,
                lambda: None,  # Placeholder function for SSH execution
            )

            handle.set_server_info(
                config.server_id, config.host, config.port, config.server_type
            )
            # Track server handle before starting
            self.server_handles[config.server_id] = handle

            # Start the process using the standard start_process method
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
        Start ChildController server using pre-built boot command.

        Args:
            config: ChildController server configuration (only server_id is used)

        Returns:
            ProcessHandle for the remote ChildController server process

        Raises:
            RuntimeError: If server cannot be started
        """
        try:
            # Create process handle
            handle = self.connection_manager.create_process(
                config.server_id,
                lambda: None,  # Placeholder function for SSH execution
            )

            handle.set_server_info(
                config.server_id, config.host, config.port, config.server_type
            )

            # Track server handle before starting
            self.server_handles[config.server_id] = handle

            # Start the process using the standard start_process method
            self.connection_manager.start_process(handle)

            return handle

        except Exception as e:
            # Clean up tracking on failure
            if config.server_id in self.server_handles:
                del self.server_handles[config.server_id]
            raise RuntimeError(
                f"Failed to start ChildController server {config.server_id}: {e}"
            )

    def stop_server(self, server_id: str, timeout: float = 10.0) -> None:
        """
        Stop a running gRPC server gracefully.

        Args:
            server_id: ID of the server to stop
            timeout: Maximum time to wait for graceful shutdown
        """
        if server_id not in self.server_handles:
            return

        print(f"Stopping server {server_id}...")
        handle = self.server_handles[server_id]

        # Use connection manager to stop the SSH process
        self.connection_manager.stop_process(handle, timeout=timeout)

        # Remove from tracking
        del self.server_handles[server_id]
        print(f"Server {server_id} stopped and removed from tracking")

    def stop_all_servers(self, timeout: float = 10.0) -> None:
        """
        Stop all managed gRPC servers.

        Args:
            timeout: Maximum time to wait for each server to stop
        """
        if not self.server_handles:
            print("No servers to stop")
            return

        print(f"Stopping {len(self.server_handles)} servers...")
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
            ProcessHandle if server exists, None otherwise
        """
        return self.server_handles.get(server_id)

    def cleanup(self) -> None:
        """Clean up all resources and stop remaining servers."""
        print("SSH gRPC server manager cleanup starting...")
        self.stop_all_servers()
        self.connection_manager.cleanup()
        print("SSH gRPC server manager cleanup completed")
