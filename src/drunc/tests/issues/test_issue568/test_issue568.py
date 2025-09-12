#!/usr/bin/env python3
"""
Multi-Process gRPC Tree Structure Classes

Architecture:
- Test Process: Manager client only
- Separate processes: Manager server, RootController server+client, ChildController servers+clients
- Manager client coordinates communication and process management

All inter-component communication happens via gRPC across process boundaries.
"""

import multiprocessing
import os
import signal
import time
from concurrent import futures
from typing import Dict, List, Tuple

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
SERVER_GRACE_PERIOD = 2

# Base port assignments for dynamic allocation
BASE_MANAGER_PORT = 50070
BASE_ROOT_PORT = 50071
BASE_CHILD_PORT = 50072


def run_manager_server(
    server_port: int,
    server_options: List[Tuple[str, any]] = None,
    ready_event: multiprocessing.Event = None,
    stop_event: multiprocessing.Event = None,
) -> None:
    """
    Standalone function to run Manager server in a separate process.

    Args:
        server_port: Port number for the Manager's gRPC server
        server_options: List of gRPC server configuration tuples
        ready_event: Event to signal when server is ready (optional)
        stop_event: Event to signal server shutdown (optional)
    """

    def signal_handler(signum, frame):
        """Handle shutdown signals gracefully"""
        if stop_event:
            stop_event.set()

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    class ManagerServiceImpl(ManagerServiceServicer):
        def MakeRequest(self, request, context):
            return DummyResponse(reply=f"Manager server response: {request.message}")

    # Create and configure server
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=MANAGER_MAX_WORKERS),
        options=server_options or [],
    )

    add_ManagerServiceServicer_to_server(ManagerServiceImpl(), server)
    port = server.add_insecure_port(f"[::]:{server_port}")

    try:
        server.start()
        print(f"Manager server started on port {port}")

        # Signal that server is ready
        if ready_event:
            ready_event.set()

        # Wait for stop signal or run indefinitely
        if stop_event:
            while not stop_event.is_set():
                time.sleep(0.1)
        else:
            server.wait_for_termination()

    except Exception as e:
        print(f"Manager server error: {e}")
    finally:
        print("Shutting down Manager server...")
        server.stop(grace=SERVER_GRACE_PERIOD)


def run_root_controller_server(
    server_port: int,
    manager_port: int,
    server_options: List[Tuple[str, any]] = None,
    client_options: List[Tuple[str, any]] = None,
    ready_event: multiprocessing.Event = None,
    stop_event: multiprocessing.Event = None,
) -> None:
    """
    Standalone function to run RootController server with Manager client in a separate process.

    Args:
        server_port: Port number for the RootController's gRPC server
        manager_port: Port number of the Manager server to connect to
        server_options: List of gRPC server configuration tuples
        client_options: List of gRPC client configuration tuples
        ready_event: Event to signal when server is ready (optional)
        stop_event: Event to signal server shutdown (optional)
    """

    def signal_handler(signum, frame):
        """Handle shutdown signals gracefully"""
        if stop_event:
            stop_event.set()

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    class RootControllerServiceImpl(RootControllerServiceServicer):
        def MakeRequest(self, request, context):
            return DummyResponse(
                reply=f"RootController server response: {request.message}"
            )

    # Create and configure server
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=CONTROLLER_MAX_WORKERS),
        options=server_options or [],
    )

    add_RootControllerServiceServicer_to_server(RootControllerServiceImpl(), server)
    port = server.add_insecure_port(f"[::]:{server_port}")

    # Set up client connection to Manager
    manager_channel = None
    manager_stub = None

    try:
        server.start()

        # Establish connection to Manager
        manager_channel = grpc.insecure_channel(
            f"localhost:{manager_port}", options=client_options or []
        )
        manager_stub = ManagerServiceStub(manager_channel)

        print(
            f"RootController server started on port {port}, connected to Manager on {manager_port}"
        )

        # Signal that server is ready
        if ready_event:
            ready_event.set()

        # Wait for stop signal or run indefinitely
        if stop_event:
            while not stop_event.is_set():
                time.sleep(0.1)
        else:
            server.wait_for_termination()

    except Exception as e:
        print(f"RootController server error: {e}")
    finally:
        print("Shutting down RootController server...")
        if manager_channel:
            manager_channel.close()
        server.stop(grace=SERVER_GRACE_PERIOD)


def run_child_controller_server(
    server_port: int,
    root_port: int,
    child_name: str,
    server_options: List[Tuple[str, any]] = None,
    client_options: List[Tuple[str, any]] = None,
    ready_event: multiprocessing.Event = None,
    stop_event: multiprocessing.Event = None,
) -> None:
    """
    Standalone function to run ChildController server with RootController client in a separate process.

    Args:
        server_port: Port number for the ChildController's gRPC server
        root_port: Port number of the RootController server to connect to
        child_name: Unique identifier for this child controller
        server_options: List of gRPC server configuration tuples
        client_options: List of gRPC client configuration tuples
        ready_event: Event to signal when server is ready (optional)
        stop_event: Event to signal server shutdown (optional)
    """

    def signal_handler(signum, frame):
        """Handle shutdown signals gracefully"""
        if stop_event:
            stop_event.set()

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    class ChildControllerServiceImpl(ChildControllerServiceServicer):
        def __init__(self, name: str):
            self.name = name

        def MakeRequest(self, request, context):
            return DummyResponse(
                reply=f"{self.name} server response: {request.message}"
            )

    # Create and configure server
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=CONTROLLER_MAX_WORKERS),
        options=server_options or [],
    )

    add_ChildControllerServiceServicer_to_server(
        ChildControllerServiceImpl(child_name), server
    )
    port = server.add_insecure_port(f"[::]:{server_port}")

    # Set up client connection to RootController
    root_channel = None
    root_stub = None

    try:
        server.start()

        # Establish connection to RootController
        root_channel = grpc.insecure_channel(
            f"localhost:{root_port}", options=client_options or []
        )
        root_stub = RootControllerServiceStub(root_channel)

        print(
            f"{child_name} server started on port {port}, connected to RootController on {root_port}"
        )

        # Signal that server is ready
        if ready_event:
            ready_event.set()

        # Wait for stop signal or run indefinitely
        if stop_event:
            while not stop_event.is_set():
                time.sleep(0.1)
        else:
            server.wait_for_termination()

    except Exception as e:
        print(f"{child_name} server error: {e}")
    finally:
        print(f"Shutting down {child_name} server...")
        if root_channel:
            root_channel.close()
        server.stop(grace=SERVER_GRACE_PERIOD)


class ProcessManagerClient:
    """
    Client-only Manager class that coordinates communication with all components
    running in separate processes. This runs in the test process.
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
            client_options: List of gRPC client configuration tuples
        """
        self.manager_port = manager_port
        self.root_port = root_port
        self.child_ports = child_ports
        self.client_options = client_options or []

        # Client connections
        self.manager_channel = None
        self.manager_stub = None
        self.root_channel = None
        self.root_stub = None
        self.child_channels = {}
        self.child_stubs = {}

    def connect_to_all_servers(self) -> None:
        """Establish gRPC client connections to all servers."""
        # Connect to Manager
        self.manager_channel = grpc.insecure_channel(
            f"localhost:{self.manager_port}", options=self.client_options
        )
        self.manager_stub = ManagerServiceStub(self.manager_channel)

        # Connect to RootController
        self.root_channel = grpc.insecure_channel(
            f"localhost:{self.root_port}", options=self.client_options
        )
        self.root_stub = RootControllerServiceStub(self.root_channel)

        # Connect to all ChildControllers
        for i, port in enumerate(self.child_ports):
            child_name = f"ChildController{i + 1}"
            channel = grpc.insecure_channel(
                f"localhost:{port}", options=self.client_options
            )
            stub = ChildControllerServiceStub(channel)

            self.child_channels[child_name] = channel
            self.child_stubs[child_name] = stub

        # Allow connections to establish
        time.sleep(0.5)

    def talk_to_manager(self) -> DummyResponse:
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

    def talk_to_root_controller(self) -> DummyResponse:
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

    def talk_to_child_controller(self, child_name: str) -> DummyResponse:
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

    def talk_to_all_child_controllers(self) -> Dict[str, DummyResponse]:
        """Send requests to all ChildController servers."""
        responses = {}
        for child_name in self.child_stubs.keys():
            responses[child_name] = self.talk_to_child_controller(child_name)
        return responses

    def perform_full_communication_test(self) -> None:
        """Perform comprehensive communication test with all components."""
        print(f"Testing communication with {len(self.child_ports)} children...")

        # Test Manager communication
        print("   ProcessManagerClient → Manager")
        response = self.talk_to_manager()
        print(f"     Response: {response.reply}")

        # Test RootController communication
        print("   ProcessManagerClient → RootController")
        response = self.talk_to_root_controller()
        print(f"     Response: {response.reply}")

        # Test all ChildController communications
        child_responses = self.talk_to_all_child_controllers()
        for child_name, response in child_responses.items():
            print(f"   ProcessManagerClient → {child_name}")
            print(f"     Response: {response.reply}")

        print("   All communications successful")

    def close_all_connections(self) -> None:
        """Close all gRPC client connections."""
        if self.manager_channel:
            self.manager_channel.close()
        if self.root_channel:
            self.root_channel.close()

        for channel in self.child_channels.values():
            channel.close()

        # Clear connection references
        self.child_channels.clear()
        self.child_stubs.clear()


def create_grpc_tree_processes(
    number_of_children: int,
    manager_server_config: List[Tuple[str, any]],
    manager_client_config: List[Tuple[str, any]],
    root_server_config: List[Tuple[str, any]],
    root_client_config: List[Tuple[str, any]],
    child_server_config: List[Tuple[str, any]],
    child_client_config: List[Tuple[str, any]],
) -> Tuple[ProcessManagerClient, List[multiprocessing.Process]]:
    """
    Create a multi-process gRPC tree structure.

    Spawns separate processes for Manager server, RootController server, and ChildController servers.
    Returns a ProcessManagerClient that can communicate with all components.

    Args:
        number_of_children: Number of child controllers to create
        manager_server_config: List of gRPC options for Manager's server
        manager_client_config: List of gRPC options for Manager's client
        root_server_config: List of gRPC options for RootController's server
        root_client_config: List of gRPC options for RootController's client
        child_server_config: List of gRPC options for ChildController servers
        child_client_config: List of gRPC options for ChildController clients

    Returns:
        Tuple of (ProcessManagerClient, list of Process objects)

    Raises:
        ValueError: If number_of_children is less than 0
    """
    if number_of_children < 0:
        raise ValueError("Number of children must be non-negative")

    # Calculate port assignments
    manager_port = BASE_MANAGER_PORT
    root_port = BASE_ROOT_PORT
    child_ports = [BASE_CHILD_PORT + i for i in range(number_of_children)]

    processes = []
    ready_events = []
    stop_events = []

    # Create Manager server process
    manager_ready = multiprocessing.Event()
    manager_stop = multiprocessing.Event()
    manager_process = multiprocessing.Process(
        target=run_manager_server,
        args=(manager_port, manager_server_config, manager_ready, manager_stop),
        name="ManagerServer",
    )

    processes.append(manager_process)
    ready_events.append(manager_ready)
    stop_events.append(manager_stop)

    # Create RootController server process
    root_ready = multiprocessing.Event()
    root_stop = multiprocessing.Event()
    root_process = multiprocessing.Process(
        target=run_root_controller_server,
        args=(
            root_port,
            manager_port,
            root_server_config,
            root_client_config,
            root_ready,
            root_stop,
        ),
        name="RootControllerServer",
    )

    processes.append(root_process)
    ready_events.append(root_ready)
    stop_events.append(root_stop)

    # Create ChildController server processes
    for i in range(number_of_children):
        child_port = child_ports[i]
        child_name = f"ChildController{i + 1}"
        child_ready = multiprocessing.Event()
        child_stop = multiprocessing.Event()

        child_process = multiprocessing.Process(
            target=run_child_controller_server,
            args=(
                child_port,
                root_port,
                child_name,
                child_server_config,
                child_client_config,
                child_ready,
                child_stop,
            ),
            name=f"ChildServer{i + 1}",
        )

        processes.append(child_process)
        ready_events.append(child_ready)
        stop_events.append(child_stop)

    # Start all processes
    print(f"Starting {len(processes)} server processes...")
    for process in processes:
        process.start()

    # Wait for all servers to be ready
    print("Waiting for all servers to be ready...")
    for ready_event in ready_events:
        ready_event.wait(timeout=10)

    # Allow extra time for servers to be fully ready
    time.sleep(1)

    # Create ProcessManagerClient
    process_manager = ProcessManagerClient(
        manager_port=manager_port,
        root_port=root_port,
        child_ports=child_ports,
        client_options=manager_client_config,
    )

    # Store stop events in the process manager for cleanup
    process_manager._stop_events = stop_events

    return process_manager, processes


def stop_all_processes(
    processes: List[multiprocessing.Process],
    stop_events: List[multiprocessing.Event] = None,
    timeout: int = 10,
) -> None:
    """
    Stop all processes gracefully with proper cleanup.

    Args:
        processes: List of Process objects to terminate
        stop_events: Optional list of Events to signal graceful shutdown
        timeout: Maximum time to wait for processes to terminate
    """
    print("Shutting down all processes...")

    # Signal graceful shutdown if events are available
    if stop_events:
        for stop_event in stop_events:
            stop_event.set()

    # Wait for processes to terminate gracefully
    start_time = time.time()
    for process in processes:
        remaining_time = max(0, timeout - (time.time() - start_time))
        process.join(timeout=remaining_time)

        # Force termination if process didn't exit gracefully
        if process.is_alive():
            print(f"Force terminating process {process.name}")
            process.terminate()
            process.join(timeout=2)

            # Last resort: kill the process
            if process.is_alive():
                print(f"Force killing process {process.name}")
                process.kill()
                process.join()


def enable_verbose_logging():
    """Enable verbose gRPC logging for debugging."""
    os.environ["GRPC_VERBOSITY"] = "DEBUG"
    os.environ["GRPC_TRACE"] = "http"


def test_multiprocess_http2_ping_timeout():
    """
    Test HTTP/2 ping timeout errors using multi-process architecture.

    Creates a scenario where components run in separate processes and communicate
    via gRPC with aggressive keepalive settings to trigger ping timeout behaviour.
    """
    # enable_verbose_logging()

    print("=" * 60)
    print("Multi-Process HTTP/2 Ping Timeout Test")
    print("=" * 60)

    # Aggressive keepalive settings to trigger ping timeout scenarios
    aggressive_config = [
        # ("grpc.keepalive_time_ms", 10),
        #     ("grpc.keepalive_timeout_ms", 1),
        #     (
        #         "grpc.keepalive_permit_without_calls",
        #         1,
        #     ),  # Allow keepalive without active calls
        #     ("grpc.http2.max_pings_without_data", 0),  # No limit on pings without data
        #  ("grpc.http2.min_time_between_pings_ms", 100),  # Minimum 100ms between pings
    ]

    try:
        print("Creating multi-process gRPC tree...")
        process_manager, processes = create_grpc_tree_processes(
            number_of_children=5,
            manager_server_config=aggressive_config,
            manager_client_config=aggressive_config,
            root_server_config=aggressive_config,
            root_client_config=aggressive_config,
            child_server_config=aggressive_config,
            child_client_config=aggressive_config,
        )

        print("Connecting ProcessManagerClient to all servers...")
        process_manager.connect_to_all_servers()

        print("Going idle to monitor for ping timeout behaviour...")
        time.sleep(150)
        print("Finished idle period.")

        # Perform initial communication test
        process_manager.perform_full_communication_test()

        print("Going idle to monitor for ping timeout behaviour...")
        time.sleep(150)
        print("Finished idle period.")

        print("Testing communication after monitoring period...")
        process_manager.perform_full_communication_test()

        print("Test completed - check logs for ping_timeout messages")

    except Exception as e:
        print(f"Test encountered error: {e}")
        raise
    finally:
        # Clean up
        if "process_manager" in locals():
            process_manager.close_all_connections()

        if "processes" in locals():
            stop_events = getattr(process_manager, "_stop_events", None)
            stop_all_processes(processes, stop_events)


if __name__ == "__main__":
    test_multiprocess_http2_ping_timeout()
