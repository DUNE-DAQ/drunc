"""
A class for managing gRPC server processes started
with multiprocessing.
Consists of a Manager server, a root controller, and multiple child controllers.
"""

import time
from typing import Dict, List, Tuple

from drunc.tests.grpc.grpc_independent_root_controller_client import (
    IndependentRootControllerClient,
)
from drunc.tests.grpc_testing_tools.available_grpc_servers import ServerType
from drunc.tests.grpc_testing_tools.grpc_log_file_manager import LogFileManager
from drunc.tests.grpc_testing_tools.grpc_log_util import (
    stderr_observer,
    stdout_observer,
)
from drunc.tests.grpc_testing_tools.grpc_server_manager import (
    GrpcServerConfig,
    GrpcServerManager,
)
from drunc.tests.grpc_testing_tools.grpc_testing_ports import (
    BASE_CHILD_PORT,
    BASE_MANAGER_PORT,
    BASE_ROOT_PORT,
    MAX_CHILDREN,
)
from drunc.tests.grpc_testing_tools.multiprocessing_connection_manager import (
    MultiprocessingConnectionManager,
)

# Import generated gRPC code
from drunc.tests.grpc_testing_tools.test_services_pb2 import DummyRequest
from drunc.tests.grpc_testing_tools.test_services_pb2_grpc import (
    ChildControllerServiceStub,
    ManagerServiceStub,
    RootControllerServiceStub,
)


class ProcessManagerClient:
    """
    Client for coordinating communication with all gRPC tree components.

    Manages connections to all server processes and provides unified
    interface for testing communication across the entire tree structure.
    This class remains unchanged as it's purely client-side logic.
    """

    def __init__(
        self,
        manager_port: int,
        root_port: int,
        child_ports: List[int],
        client_options: List[Tuple[str, any]] = None,
    ):
        """
        Initialise ProcessManagerClient with connection details.

        Args:
            manager_port: Port of the Manager server
            root_port: Port of the RootController server
            child_ports: List of ChildController server ports
            client_options: List of gRPC client configuration options
        """
        self.manager_port = manager_port
        self.root_port = root_port
        self.child_ports = child_ports
        self.client_options = client_options or []

        # Client connection state
        self.manager_channel = None
        self.manager_stub = None
        self.root_channel = None
        self.root_stub = None
        self.child_channels = {}
        self.child_stubs = {}

    def connect_to_all_servers(self) -> None:
        """Establish gRPC client connections to all servers in the tree."""
        import grpc

        # Connect to Manager server
        self.manager_channel = grpc.insecure_channel(
            f"localhost:{self.manager_port}", options=self.client_options
        )
        self.manager_stub = ManagerServiceStub(self.manager_channel)

        # Connect to RootController server
        self.root_channel = grpc.insecure_channel(
            f"localhost:{self.root_port}", options=self.client_options
        )
        self.root_stub = RootControllerServiceStub(self.root_channel)

        # Connect to all ChildController servers
        for i, port in enumerate(self.child_ports):
            child_name = f"ChildController{i + 1}"
            channel = grpc.insecure_channel(
                f"localhost:{port}", options=self.client_options
            )
            stub = ChildControllerServiceStub(channel)

            self.child_channels[child_name] = channel
            self.child_stubs[child_name] = stub

        # Allow time for connections to be established
        time.sleep(0.5)

    def talk_to_manager(self):
        """Send a request to the Manager server."""
        if not self.manager_stub:
            raise RuntimeError(
                "Manager connection not established. Call connect_to_all_servers() first."
            )

        request = DummyRequest(
            message="Hello from ProcessManagerClient to Manager",
            timestamp=int(time.time() * 1000),
        )
        return self.manager_stub.MakeRequest(request)

    def talk_to_root_controller(self):
        """Send a request to the RootController server."""
        if not self.root_stub:
            raise RuntimeError(
                "RootController connection not established. Call connect_to_all_servers() first."
            )

        request = DummyRequest(
            message="Hello from ProcessManagerClient to RootController",
            timestamp=int(time.time() * 1000),
        )
        return self.root_stub.MakeRequest(request)

    def talk_to_child_controller(self, child_name: str):
        """Send a request to a specific ChildController server."""
        if child_name not in self.child_stubs:
            raise RuntimeError(
                f"No connection to ChildController '{child_name}'. Call connect_to_all_servers() first."
            )

        request = DummyRequest(
            message=f"Hello from ProcessManagerClient to {child_name}",
            timestamp=int(time.time() * 1000),
        )
        return self.child_stubs[child_name].MakeRequest(request)

    def talk_to_all_child_controllers(self) -> Dict[str, any]:
        """Send requests to all ChildController servers."""
        responses = {}
        for child_name in self.child_stubs.keys():
            responses[child_name] = self.talk_to_child_controller(child_name)
        return responses

    def perform_full_communication_test(self) -> None:
        """Perform comprehensive communication test with all components."""
        print(f"Testing communication with {len(self.child_ports)} children...")

        self.talk_to_manager()
        self.talk_to_root_controller()
        self.talk_to_all_child_controllers()

        print("   All communications successful")

    def close_all_connections(self) -> None:
        """Close all gRPC client connections and cleanup resources."""
        if self.manager_channel:
            self.manager_channel.close()
        if self.root_channel:
            self.root_channel.close()

        for channel in self.child_channels.values():
            channel.close()

        # Clear connection references
        self.child_channels.clear()
        self.child_stubs.clear()


class GrpcProcessTreeManager:
    """
    Context manager for multi-process gRPC tree lifecycle management using server manager abstraction.
    """

    def __init__(
        self,
        server_manager: GrpcServerManager,
        number_of_children: int,
        manager_max_workers: int,
        controller_max_workers: int,
        manager_server_config: List[Tuple[str, any]] = None,
        manager_client_config: List[Tuple[str, any]] = None,
        root_server_config: List[Tuple[str, any]] = None,
        root_client_config: List[Tuple[str, any]] = None,
        child_server_config: List[Tuple[str, any]] = None,
        child_client_config: List[Tuple[str, any]] = None,
    ):
        """
        Initialise GrpcProcessTreeManager with server manager and configuration.

        Args:
            server_manager: GrpcServerManager implementation for server lifecycle
            number_of_children: Number of child controllers to create
            manager_max_workers: Maximum worker threads for Manager server
            controller_max_workers: Maximum worker threads for Controller servers
            manager_server_config: gRPC options for Manager's server
            manager_client_config: gRPC options for Manager's client
            root_server_config: gRPC options for RootController's server
            root_client_config: gRPC options for RootController's client
            child_server_config: gRPC options for ChildController servers
            child_client_config: gRPC options for ChildController clients
        """
        assert number_of_children < MAX_CHILDREN, (
            f"Number of children must be less than {MAX_CHILDREN} to ensure correct port cleanup."
        )
        if number_of_children < 0:
            raise ValueError("Number of children must be non-negative")

        self.server_manager = server_manager
        self.manager_max_workers = manager_max_workers
        self.controller_max_workers = controller_max_workers
        self.number_of_children = number_of_children
        self.manager_server_config = manager_server_config or []
        self.manager_client_config = manager_client_config or []
        self.root_server_config = root_server_config or []
        self.root_client_config = root_client_config or []
        self.child_server_config = child_server_config or []
        self.child_client_config = child_client_config or []

        # Calculate port assignments
        self.manager_port = BASE_MANAGER_PORT
        self.root_port = BASE_ROOT_PORT
        self.child_ports = [BASE_CHILD_PORT + i for i in range(number_of_children)]

        # Runtime state
        self.server_ids: List[str] = []
        self.process_manager = None
        self.log_file_manager = LogFileManager()
        self.direct_clients = {}

    def __enter__(self):
        """
        Set up gRPC tree with all processes and return configured client.

        Returns:
            ProcessManagerClient: Client for communicating with all servers
        """
        return self._setup_tree()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Clean up all resources including processes, connections, and log files.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        self._cleanup_tree()

    def _setup_tree(self) -> ProcessManagerClient:
        """
        Internal method to create and start all server processes using server manager.

        Returns:
            ProcessManagerClient: Client for communicating with all servers
        """
        log_file = self.log_file_manager.create_log_file("TreeClient")
        stderr_observer(log_file)
        stdout_observer(log_file)

        # Create Manager server
        manager_config = GrpcServerConfig(
            server_id="ManagerServer",
            server_type=ServerType.MANAGER,
            host="localhost",
            port=self.manager_port,
            max_workers=self.manager_max_workers,
            log_file=self.log_file_manager.create_log_file("ManagerServer"),
            server_options=self.manager_server_config,
            client_options=self.manager_client_config,
        )

        self.server_manager.start_manager_server(manager_config)
        self.server_ids.append("ManagerServer")

        # Create RootController server
        root_config = GrpcServerConfig(
            server_id="RootControllerServer",
            server_type=ServerType.ROOT_CONTROLLER,
            host="localhost",
            port=self.root_port,
            max_workers=self.controller_max_workers,
            log_file=self.log_file_manager.create_log_file("RootControllerServer"),
            server_options=self.root_server_config,
            client_options=self.root_client_config,
            manager_port=self.manager_port,
        )

        self.server_manager.start_root_controller_server(root_config)
        self.server_ids.append("RootControllerServer")

        # Create ChildController servers
        for i in range(self.number_of_children):
            child_port = self.child_ports[i]
            child_name = f"ChildController{i + 1}"
            child_server_id = f"ChildServer{i + 1}"

            child_config = GrpcServerConfig(
                server_id=child_server_id,
                server_type=ServerType.CHILD_CONTROLLER,
                host="localhost",
                port=child_port,
                max_workers=self.controller_max_workers,
                log_file=self.log_file_manager.create_log_file(child_server_id),
                server_options=self.child_server_config,
                client_options=self.child_client_config,
                root_port=self.root_port,
                child_name=child_name,
            )

            self.server_manager.start_child_controller_server(child_config)
            self.server_ids.append(child_server_id)

        # Wait for all servers to be ready
        print("Waiting for all servers to be ready...")
        for server_id in self.server_ids:
            ready = self.server_manager.wait_for_server_ready(server_id, timeout=10.0)
            if not ready:
                print(
                    f"Warning: Server {server_id} did not signal ready within timeout"
                )

        # Create and return ProcessManagerClient
        self.process_manager = ProcessManagerClient(
            manager_port=self.manager_port,
            root_port=self.root_port,
            child_ports=self.child_ports,
            client_options=self.manager_client_config,
        )

        return self.process_manager

    def _cleanup_tree(self) -> None:
        """Internal method to clean up all resources using server manager."""
        # Clean up direct clients
        if hasattr(self, "direct_clients"):
            for client_id in list(self.direct_clients.keys()):
                self.remove_direct_client(client_id)

        # Close client connections
        if self.process_manager:
            self.process_manager.close_all_connections()

        # Stop all servers using server manager
        self._stop_all_processes()

        # Clean up server manager
        self.server_manager.cleanup()

        # Clean up log files
        self.log_file_manager.cleanup()

    def _stop_all_processes(self) -> None:
        """Stop all server processes gracefully using server manager."""
        print("Shutting down all servers...")

        # Stop all servers through server manager
        self.server_manager.stop_all_servers(timeout=10.0)

        # Clear server tracking
        self.server_ids.clear()

    def check_for_errors(self):
        """
        Check if any gRPC errors have been detected in log files.

        Returns:
            Error details if found, None otherwise
        """
        return self.log_file_manager.check_for_errors()

    def get_root_port(self) -> int:
        """
        Get the port number of the RootController server.

        Returns:
            Port number where RootController is listening
        """
        return self.root_port

    def create_direct_client(
        self, client_id: str = None, client_options: List[Tuple[str, any]] = None
    ) -> IndependentRootControllerClient:
        """
        Create and manage a DirectRootClient with automatic lifecycle management.

        Creates a DirectRootClient, establishes its connection, adds it to log monitoring,
        and tracks it for automatic cleanup. The client is ready to use immediately.

        Args:
            client_id: Unique identifier for the client (auto-generated if None)
            client_options: gRPC client configuration options (uses root_client_config if None)

        Returns:
            IndependentRootControllerClient: Ready-to-use direct client connection

        Raises:
            RuntimeError: If tree is not active or client creation fails
        """
        # Generate client ID if not provided
        if client_id is None:
            client_id = f"DirectClient{len(self.direct_clients) + 1}"

        # Check for duplicate client IDs
        if client_id in self.direct_clients:
            raise ValueError(f"DirectClient with ID '{client_id}' already exists")

        # Use provided options or fall back to tree's root client config
        effective_options = (
            client_options if client_options is not None else self.root_client_config
        )

        try:
            # Create DirectRootClient instance
            direct_client = IndependentRootControllerClient(
                client_id=client_id,
                root_port=self.root_port,
                client_options=effective_options,
            )

            # Create log file for the client using existing infrastructure
            client_log_file = self.log_file_manager.create_log_file(
                f"DirectClient_{client_id}"
            )

            # Establish connection with stderr redirection
            direct_client._connect_with_stderr_redirect(client_log_file)

            # Track the client for lifecycle management
            self.direct_clients[client_id] = direct_client

            print(
                f"Created and connected DirectRootClient '{client_id}' (log: {client_log_file})"
            )
            return direct_client

        except Exception as e:
            # Clean up on failure
            if client_id in self.direct_clients:
                del self.direct_clients[client_id]
            raise RuntimeError(f"Failed to create DirectRootClient '{client_id}': {e}")

    def remove_direct_client(self, client_id: str) -> bool:
        """
        Remove and cleanup a managed DirectRootClient.

        Args:
            client_id: ID of the client to remove

        Returns:
            True if client was found and removed, False if not found
        """
        if client_id not in self.direct_clients:
            return False

        direct_client = self.direct_clients[client_id]

        try:
            # Disconnect and cleanup the client
            direct_client._disconnect()
            print(f"Removed DirectRootClient '{client_id}'")
        except Exception as e:
            print(f"Error during DirectRootClient '{client_id}' cleanup: {e}")
        finally:
            # Remove from tracking regardless of cleanup errors
            del self.direct_clients[client_id]

        return True

    def get_direct_client(self, client_id: str) -> IndependentRootControllerClient:
        """
        Get a managed DirectRootClient by ID.

        Args:
            client_id: ID of the client to retrieve

        Returns:
            IndependentRootControllerClient instance

        Raises:
            KeyError: If client with given ID doesn't exist
        """
        if client_id not in self.direct_clients:
            raise KeyError(f"No DirectRootClient with ID '{client_id}' found")

        return self.direct_clients[client_id]

    def list_direct_clients(self) -> List[str]:
        """
        List IDs of all managed DirectRootClient instances.

        Returns:
            List of client IDs
        """
        return list(self.direct_clients.keys())

    @classmethod
    def create_with_multiprocessing(
        cls,
        number_of_children: int,
        manager_max_workers: int,
        controller_max_workers: int,
        env_vars: Dict[str, str] = None,
        **kwargs,
    ) -> "GrpcProcessTreeManager":
        """
        Factory method to create GrpcProcessTreeManager with multiprocessing server manager.
        """

        connection_manager = MultiprocessingConnectionManager(env_vars=env_vars)
        server_manager = GrpcServerManager(connection_manager)

        return cls(
            server_manager=server_manager,
            number_of_children=number_of_children,
            manager_max_workers=manager_max_workers,
            controller_max_workers=controller_max_workers,
            **kwargs,
        )
