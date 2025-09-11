#!/usr/bin/env python3
"""
gRPC Tree Structure Classes with Communication Methods (Dynamic Children Support)

Classes for hierarchical gRPC setup:
Manager ↔ RootController ↔ ChildController (configurable number of instances)

All channels are established during initialisation for efficient communication.
The number of child controllers can be specified dynamically in each test case.
"""

import time
from concurrent import futures

import grpc

# Import generated gRPC code
from drunc.tests.issues.test_issue568.test_pb2 import DummyRequest, DummyResponse
from drunc.tests.issues.test_issue568.test_pb2_grpc import (
    ChildControllerServiceServicer,
    ChildControllerServiceStub,
    ManagerServiceServicer,
    ManagerServiceStub,
    RootControllerServiceServicer,
    RootControllerServiceStub,
    add_ChildControllerServiceServicer_to_server,
    add_ManagerServiceServicer_to_server,
    add_RootControllerServiceServicer_to_server,
)

# Configuration constants
MANAGER_MAX_WORKERS = 10
CONTROLLER_MAX_WORKERS = 1

# Base port assignments for dynamic allocation
BASE_MANAGER_PORT = 50070
BASE_ROOT_PORT = 50071
BASE_CHILD_PORT = 50072


class Manager:
    """
    Manager class with gRPC server and client capabilities.
    Communicates bidirectionally with RootController.
    """

    def __init__(
        self, server_port, root_port, server_options=None, client_options=None
    ):
        """
        Initialise Manager with server and client configurations.

        Args:
            server_port (int): Port for Manager's gRPC server
            root_port (int): Port of RootController to connect to
            server_options (list): List of gRPC server configuration tuples
            client_options (list): List of gRPC client configuration tuples
        """
        self.server_port = server_port
        self.root_port = root_port
        self.server_options = server_options or []
        self.client_options = client_options or []

        self.server = None
        self.channel = None
        self.stub = None

    def start_server(self):
        """Start the Manager's gRPC server with configured options"""

        class ManagerServiceImpl(ManagerServiceServicer):
            def MakeRequest(self, request, context):
                return DummyResponse(reply=f"Manager response: {request.message}")

        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=MANAGER_MAX_WORKERS),
            options=self.server_options,
        )

        add_ManagerServiceServicer_to_server(ManagerServiceImpl(), self.server)
        self.server.add_insecure_port(f"localhost:{self.server_port}")
        self.server.start()
        time.sleep(0.1)  # Allow server to fully start

    def start_client(self):
        """Start the Manager's gRPC client connection to RootController"""
        self.channel = grpc.insecure_channel(
            f"localhost:{self.root_port}", options=self.client_options
        )
        self.stub = RootControllerServiceStub(self.channel)
        time.sleep(0.1)  # Allow connection to establish

    def talk_to_root_controller(self, root_controller):
        """
        Send a request to the RootController.

        Args:
            root_controller (RootController): RootController instance for identification

        Returns:
            DummyResponse: Response from the RootController

        Raises:
            grpc.RpcError: If the gRPC call fails
            RuntimeError: If client connection not established
        """
        if not self.stub:
            raise RuntimeError(
                "Client connection not established. Call start_client() first."
            )

        # Create request with identifying information
        request = DummyRequest(
            message=f"Hello from Manager to RootController:{root_controller.server_port}",
            timestamp=int(time.time() * 1000),  # Current time in milliseconds
        )

        # Send the request and return the response
        response = self.stub.MakeRequest(request)
        return response

    def stop(self):
        """Stop both server and client connections gracefully"""
        if self.server:
            self.server.stop(grace=1)
        if self.channel:
            self.channel.close()


class RootController:
    """
    RootController class with gRPC server and client capabilities.
    Communicates with Manager and multiple ChildControllers.
    """

    def __init__(
        self, server_port, manager_port, server_options=None, client_options=None
    ):
        """
        Initialise RootController with server and client configurations.

        Args:
            server_port (int): Port for RootController's gRPC server
            manager_port (int): Port of Manager to connect to
            server_options (list): List of gRPC server configuration tuples
            client_options (list): List of gRPC client configuration tuples
        """
        self.server_port = server_port
        self.manager_port = manager_port
        self.server_options = server_options or []
        self.client_options = client_options or []

        self.server = None
        self.manager_channel = None
        self.manager_stub = None

        # Dictionary to store child controller connections by their names
        self.child_connections = {}

    def start_server(self):
        """Start the RootController's gRPC server with configured options"""

        class RootControllerServiceImpl(RootControllerServiceServicer):
            def MakeRequest(self, request, context):
                return DummyResponse(
                    reply=f"RootController response: {request.message}"
                )

        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=CONTROLLER_MAX_WORKERS),
            options=self.server_options,
        )

        add_RootControllerServiceServicer_to_server(
            RootControllerServiceImpl(), self.server
        )
        self.server.add_insecure_port(f"localhost:{self.server_port}")
        self.server.start()
        time.sleep(0.1)

    def start_client(self):
        """Start the RootController's gRPC client connection to Manager"""
        self.manager_channel = grpc.insecure_channel(
            f"localhost:{self.manager_port}", options=self.client_options
        )
        self.manager_stub = ManagerServiceStub(self.manager_channel)
        time.sleep(0.1)

    def add_child_connection(self, child_controller):
        """
        Establish a connection to a ChildController.

        Args:
            child_controller (ChildController): ChildController instance to connect to
        """
        child_channel = grpc.insecure_channel(
            f"localhost:{child_controller.server_port}", options=self.client_options
        )
        child_stub = ChildControllerServiceStub(child_channel)

        self.child_connections[child_controller.name] = {
            "channel": child_channel,
            "stub": child_stub,
            "controller": child_controller,
        }
        time.sleep(0.1)  # Allow connection to establish

    def talk_to_manager(self, manager):
        """
        Send a request to the Manager.

        Args:
            manager (Manager): Manager instance for identification

        Returns:
            DummyResponse: Response from the Manager

        Raises:
            grpc.RpcError: If the gRPC call fails
            RuntimeError: If Manager client connection not established
        """
        if not self.manager_stub:
            raise RuntimeError(
                "Manager client connection not established. Call start_client() first."
            )

        # Create request with identifying information
        request = DummyRequest(
            message=f"Hello from RootController:{self.server_port} to Manager",
            timestamp=int(time.time() * 1000),  # Current time in milliseconds
        )

        # Send the request and return the response
        response = self.manager_stub.MakeRequest(request)
        return response

    def talk_to_child(self, child_controller):
        """
        Send a request to a ChildController using pre-established connection.

        Args:
            child_controller (ChildController): ChildController instance to communicate with

        Returns:
            DummyResponse: Response from the ChildController

        Raises:
            grpc.RpcError: If the gRPC call fails
            RuntimeError: If no connection exists to the specified child
        """
        child_name = child_controller.name

        # Check if we have a connection to this child controller
        if child_name not in self.child_connections:
            raise RuntimeError(
                f"No connection established to child controller '{child_name}'. "
                f"Call add_child_connection() first."
            )

        # Use pre-established connection
        child_stub = self.child_connections[child_name]["stub"]

        # Create request with identifying information
        request = DummyRequest(
            message=f"Hello from RootController:{self.server_port} to {child_controller.name}",
            timestamp=int(time.time() * 1000),  # Current time in milliseconds
        )

        # Send the request and return the response
        response = child_stub.MakeRequest(request)
        return response

    def talk_to_all_children(self):
        """
        Send requests to all connected child controllers.

        Returns:
            dict: Dictionary mapping child names to their responses

        Raises:
            grpc.RpcError: If any gRPC call fails
        """
        responses = {}
        for child_name, connection_info in self.child_connections.items():
            child_controller = connection_info["controller"]
            response = self.talk_to_child(child_controller)
            responses[child_name] = response
        return responses

    def get_child_count(self):
        """
        Get the number of connected child controllers.

        Returns:
            int: Number of child controllers currently connected
        """
        return len(self.child_connections)

    def stop(self):
        """Stop server and all client connections gracefully"""
        if self.server:
            self.server.stop(grace=1)
        if self.manager_channel:
            self.manager_channel.close()

        # Close all child controller connections
        for connection_info in self.child_connections.values():
            connection_info["channel"].close()

        # Clear the connections dictionary
        self.child_connections.clear()


class ChildController:
    """
    ChildController class with gRPC server and client capabilities.
    Communicates bidirectionally with RootController.
    """

    def __init__(
        self, server_port, root_port, name, server_options=None, client_options=None
    ):
        """
        Initialise ChildController with server and client configurations.

        Args:
            server_port (int): Port for ChildController's gRPC server
            root_port (int): Port of RootController to connect to
            name (str): Unique identifier for this child controller
            server_options (list): List of gRPC server configuration tuples
            client_options (list): List of gRPC client configuration tuples
        """
        self.server_port = server_port
        self.root_port = root_port
        self.name = name
        self.server_options = server_options or []
        self.client_options = client_options or []

        self.server = None
        self.channel = None
        self.stub = None

    def start_server(self):
        """Start the ChildController's gRPC server with configured options"""

        class ChildControllerServiceImpl(ChildControllerServiceServicer):
            def __init__(self, name):
                self.name = name

            def MakeRequest(self, request, context):
                return DummyResponse(reply=f"{self.name} response: {request.message}")

        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=CONTROLLER_MAX_WORKERS),
            options=self.server_options,
        )

        add_ChildControllerServiceServicer_to_server(
            ChildControllerServiceImpl(self.name), self.server
        )
        self.server.add_insecure_port(f"localhost:{self.server_port}")
        self.server.start()
        time.sleep(0.1)

    def start_client(self):
        """Start the ChildController's gRPC client connection to RootController"""
        self.channel = grpc.insecure_channel(
            f"localhost:{self.root_port}", options=self.client_options
        )
        self.stub = RootControllerServiceStub(self.channel)
        time.sleep(0.1)

    def talk_to_root_controller(self, root_controller):
        """
        Send a request to the RootController.

        Args:
            root_controller (RootController): RootController instance for identification

        Returns:
            DummyResponse: Response from the RootController

        Raises:
            grpc.RpcError: If the gRPC call fails
            RuntimeError: If client connection not established
        """
        if not self.stub:
            raise RuntimeError(
                "Client connection not established. Call start_client() first."
            )

        # Create request with identifying information
        request = DummyRequest(
            message=f"Hello from {self.name} to RootController:{root_controller.server_port}",
            timestamp=int(time.time() * 1000),  # Current time in milliseconds
        )

        # Send the request and return the response
        response = self.stub.MakeRequest(request)
        return response

    def stop(self):
        """Stop both server and client connections gracefully"""
        if self.server:
            self.server.stop(grace=1)
        if self.channel:
            self.channel.close()


def create_grpc_tree(
    number_of_children,
    manager_server_config,
    manager_client_config,
    root_server_config,
    root_client_config,
    child_server_config,
    child_client_config,
):
    """
    Create a hierarchical gRPC tree structure with configurable number of children.

    Creates one Manager, one RootController, and a specified number of ChildControllers
    with the provided gRPC options for servers and clients.

    Args:
        number_of_children (int): Number of child controllers to create
        manager_server_config (list): List of gRPC options for Manager's server
        manager_client_config (list): List of gRPC options for Manager's client
        root_server_config (list): List of gRPC options for RootController's server
        root_client_config (list): List of gRPC options for RootController's client
        child_server_config (list): List of gRPC options for ChildController servers
        child_client_config (list): List of gRPC options for ChildController clients

    Returns:
        tuple: (manager, root_controller, list_of_child_controllers)

    Raises:
        ValueError: If number_of_children is less than 0
    """
    if number_of_children < 0:
        raise ValueError("Number of children must be non-negative")

    # Dynamic port allocation to avoid conflicts
    manager_port = BASE_MANAGER_PORT
    root_port = BASE_ROOT_PORT

    # Create Manager instance
    manager = Manager(
        server_port=manager_port,
        root_port=root_port,
        server_options=manager_server_config,
        client_options=manager_client_config,
    )

    # Create RootController instance
    root_controller = RootController(
        server_port=root_port,
        manager_port=manager_port,
        server_options=root_server_config,
        client_options=root_client_config,
    )

    # Create the specified number of ChildController instances
    child_controllers = []
    for i in range(number_of_children):
        child_port = BASE_CHILD_PORT + i
        child_name = f"ChildController{i + 1}"

        child = ChildController(
            server_port=child_port,
            root_port=root_port,
            name=child_name,
            server_options=child_server_config,
            client_options=child_client_config,
        )

        child_controllers.append(child)

        # Establish connection from root controller to this child
        root_controller.add_child_connection(child)

    return manager, root_controller, child_controllers


def start_all_components(manager, root_controller, child_controllers):
    """
    Start all servers and establish all client connections for the gRPC tree.

    Args:
        manager (Manager): Manager instance to start
        root_controller (RootController): RootController instance to start
        child_controllers (list): List of ChildController instances to start
    """
    # Start all servers first to ensure they're ready for connections
    print(f"Starting {len(child_controllers) + 2} servers...")
    manager.start_server()
    root_controller.start_server()

    for child in child_controllers:
        child.start_server()

    # Allow servers to be fully ready before client connections
    time.sleep(0.5)

    # Start all client connections
    print("Establishing client connections...")
    manager.start_client()
    root_controller.start_client()

    for child in child_controllers:
        child.start_client()

    # Allow connections to establish
    time.sleep(0.5)


def stop_all_components(manager, root_controller, child_controllers):
    """
    Stop all servers and client connections gracefully.

    Args:
        manager (Manager): Manager instance to stop
        root_controller (RootController): RootController instance to stop
        child_controllers (list): List of ChildController instances to stop
    """
    print("Shutting down components...")

    # Stop child controllers first
    for child in child_controllers:
        child.stop()

    # Then stop root controller and manager
    root_controller.stop()
    manager.stop()


def perform_communication_test(manager, root_controller, child_controllers):
    """
    Perform a full communication test between all components.

    Args:
        manager (Manager): Manager instance
        root_controller (RootController): RootController instance
        child_controllers (list): List of ChildController instances
    """
    print(f"Testing communication with {len(child_controllers)} children...")

    # Manager to RootController
    print("   Manager → RootController")
    manager.talk_to_root_controller(root_controller)

    # RootController to Manager
    print("   RootController → Manager")
    root_controller.talk_to_manager(manager)

    # RootController to each ChildController
    for i, child in enumerate(child_controllers, 1):
        print(f"   RootController → ChildController{i}")
        root_controller.talk_to_child(child)

    # Each ChildController to RootController
    for i, child in enumerate(child_controllers, 1):
        print(f"   ChildController{i} → RootController")
        child.talk_to_root_controller(root_controller)

    print("   All communications successful")


def test_http2_ping_timeout():
    """
    Test to produce HTTP/2 ping timeout errors using completely default gRPC configurations.

    Creates a scenario where the HTTP/2 transport becomes unresponsive to ping frames
    while keeping the underlying connection alive.
    """
    print("=" * 60)
    print("HTTP/2 Ping Timeout Test - Default Configs Only")
    print("=" * 60)

    # Use completely default configs - no modifications
    default_config = []

    print("Creating gRPC tree with default configurations...")
    manager, root_controller, child_controllers = create_grpc_tree(
        number_of_children=10,
        manager_server_config=default_config,
        manager_client_config=default_config,
        root_server_config=default_config,
        root_client_config=default_config,
        child_server_config=default_config,
        child_client_config=default_config,
    )

    try:
        # Start all components
        start_all_components(manager, root_controller, child_controllers)

        # Verify initial communication
        perform_communication_test(manager, root_controller, child_controllers)
        print("Initial communication successful")

        # Let connections establish properly
        print("Allowing connections to establish (5 seconds)...")
        time.sleep(5)

        # Create resource exhaustion by overloading the server's thread pool
        # This should make it unresponsive to HTTP/2 ping frames while keeping connection alive
        print("Creating server resource exhaustion...")

        # Block the server's thread pool with long-running operations
        import threading

        def block_server_threads():
            """Block server threads to make it unresponsive to HTTP/2 pings"""
            time.sleep(120)  # Block for 2 minutes

        # Start multiple blocking threads to exhaust the server's thread pool
        blocking_threads = []
        for i in range(CONTROLLER_MAX_WORKERS + 2):  # Exceed the thread pool size
            thread = threading.Thread(target=block_server_threads)
            thread.daemon = True
            thread.start()
            blocking_threads.append(thread)

        print("Server threads blocked - monitoring for ping timeout (60 seconds)...")

        for i in range(180):
            time.sleep(1)

            # Periodically attempt communication to trigger HTTP/2 activity
            if i % 10 == 0 and i > 0:
                try:
                    manager.talk_to_root_controller(root_controller)
                except Exception as e:
                    print(f"Communication attempt failed: {type(e).__name__}")

            if (i + 1) % 15 == 0:
                print(f"  {i + 1} seconds elapsed...")

        print("Monitoring complete - check for ping_timeout in logs")

        perform_communication_test(manager, root_controller, child_controllers)
    finally:
        stop_all_components(manager, root_controller, child_controllers)


if __name__ == "__main__":
    test_http2_ping_timeout()
