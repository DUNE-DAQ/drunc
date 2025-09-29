import multiprocessing
from typing import Dict, Optional

from drunc.tests.grpc.grpc_server_manager import GrpcServerConfig, GrpcServerManager
from drunc.tests.grpc.process_connection_manager import RunningGrpcServer
from drunc.tests.grpc.run_grpc_services import (
    run_child_controller_server,
    run_process_manager_server,
    run_root_controller_server,
)


class MultiprocessingGrpcServerManager(GrpcServerManager):
    """
    gRPC server manager using Python multiprocessing.

    Manages gRPC servers as local processes using multiprocessing.Process,
    with ready/stop events for proper lifecycle coordination.
    """

    def __init__(self, connection_manager):
        """
        Initialise multiprocessing gRPC server manager.

        Args:
            connection_manager: Process connection manager for multiprocessing operations
        """
        super().__init__(connection_manager)

        # Track server processes and synchronisation events
        self.server_handles: Dict[str, RunningGrpcServer] = {}
        self.ready_events: Dict = {}
        self.stop_events: Dict = {}

    def _start_server_process(
        self, config: GrpcServerConfig, run_function, *additional_args
    ) -> RunningGrpcServer:
        """
        Common helper function to start any gRPC server process.

        This method handles the common pattern of:
        - Creating multiprocessing events for coordination
        - Creating and configuring the process handle
        - Tracking server handles and events
        - Starting the process

        Args:
            config: Server configuration containing standard parameters
            run_function: The specific server function to run (e.g., run_process_manager_server)
            *additional_args: Additional arguments specific to the server type

        Returns:
            ProcessHandle for the created server process
        """
        ready_event = multiprocessing.Event()
        stop_event = multiprocessing.Event()

        # Build the argument list: standard args + additional args + events
        process_args = [
            config.server_id,
            run_function,
            config.max_workers,
            config.port,
        ]
        process_args.extend(additional_args)
        process_args.extend(
            [
                config.log_file,
                config.server_options,
            ]
        )

        # Add client_options if any additional args were provided (indicates controller server)
        if additional_args:
            process_args.append(config.client_options)

        process_args.extend([ready_event, stop_event])

        # Create the process handle
        handle = self.connection_manager.create_process(*process_args)

        # Track server and events for lifecycle management
        self.server_handles[config.server_id] = handle
        self.ready_events[config.server_id] = ready_event
        self.stop_events[config.server_id] = stop_event

        # Start the process
        self.connection_manager.start_process(handle)
        handle.set_server_info(
            config.server_id, config.host, config.port, config.server_type
        )

        return handle

    def start_manager_server(self, config: GrpcServerConfig) -> RunningGrpcServer:
        """
        Start Manager server using multiprocessing.

        Args:
            config: Manager server configuration

        Returns:
            ProcessHandle for the Manager server process
        """
        return self._start_server_process(config, run_process_manager_server)

    def start_root_controller_server(
        self, config: GrpcServerConfig
    ) -> RunningGrpcServer:
        """
        Start RootController server using multiprocessing.

        Args:
            config: RootController server configuration (requires manager_port parameter)

        Returns:
            ProcessHandle for the RootController server process

        Raises:
            ValueError: If manager_port parameter is missing from config
        """
        manager_port = config.get_param("manager_port")
        if manager_port is None:
            raise ValueError("RootController server requires 'manager_port' parameter")

        return self._start_server_process(
            config, run_root_controller_server, manager_port
        )

    def start_child_controller_server(
        self, config: GrpcServerConfig
    ) -> RunningGrpcServer:
        """
        Start ChildController server using multiprocessing.

        Args:
            config: ChildController server configuration (requires root_port and child_name parameters)

        Returns:
            ProcessHandle for the ChildController server process

        Raises:
            ValueError: If root_port or child_name parameters are missing from config
        """
        root_port = config.get_param("root_port")
        child_name = config.get_param("child_name")

        if root_port is None:
            raise ValueError("ChildController server requires 'root_port' parameter")
        if child_name is None:
            raise ValueError("ChildController server requires 'child_name' parameter")

        return self._start_server_process(
            config, run_child_controller_server, root_port, child_name
        )

    def stop_server(self, server_id: str, timeout: float = 10.0) -> None:
        """
        Stop a running gRPC server gracefully.

        Args:
            server_id: ID of the server to stop
            timeout: Maximum time to wait for graceful shutdown

        Raises:
            RuntimeError: If server cannot be stopped
        """
        if server_id not in self.server_handles:
            return

        # Signal graceful shutdown if supported
        if server_id in self.stop_events:
            self.stop_events[server_id].set()

        # Use connection manager to stop the process
        handle = self.server_handles[server_id]
        self.connection_manager.stop_process(handle, timeout=timeout)

        # Clean up tracking
        del self.server_handles[server_id]
        if server_id in self.ready_events:
            del self.ready_events[server_id]
        if server_id in self.stop_events:
            del self.stop_events[server_id]

    def stop_all_servers(self, timeout: float = 10.0) -> None:
        """
        Stop all managed gRPC servers.

        Args:
            timeout: Maximum time to wait for each server to stop
        """
        # Signal all servers to stop gracefully
        for stop_event in self.stop_events.values():
            stop_event.set()

        # Stop all servers using connection manager
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
        self.stop_all_servers()
        self.connection_manager.cleanup()
