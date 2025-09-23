"""
Multi-Process gRPC Tree Structure with Log File Monitoring

Architecture:
- Manager Server: Central coordination service
- RootController Server: Primary controller with Manager client
- ChildController Servers: Leaf nodes with RootController clients
"""

import multiprocessing
import os
import re
import signal
import sys
import tempfile
import threading
import time
from concurrent import futures
from pathlib import Path
from typing import Dict, List, Tuple

# Import generated gRPC code
from drunc.tests.grpc.test_pb2 import (
    DummyRequest,
    DummyResponse,
)
from drunc.tests.grpc.test_pb2_grpc import (
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

SERVER_GRACE_PERIOD = 2

# Base port assignments for dynamic allocation
BASE_MANAGER_PORT = 50070
BASE_ROOT_PORT = 50071
BASE_CHILD_PORT = 50072

# gRPC error patterns to detect in log files
GRPC_ERROR_PATTERNS = [
    "ping_timeout",
    "keepalive.*timeout",
    "chttp2_transport.*GOAWAY",
    "GOAWAY.*UNAVAILABLE",
    "Error code.*ping_timeout",
    "grpc.*UNAVAILABLE.*ping",
    "Other threads are currently calling into gRPC"
]

def stderr_observer(log_file_name):
    r, w = os.pipe()
    stderr_fd = sys.stderr.fileno()
    os.dup2(w, stderr_fd)
    os.close(w)

    def reader():
        log_handle = open(log_file_name, "w", buffering=1)
        with os.fdopen(r) as pipe:
            for line in pipe:
                log_handle.write(f"{line.strip()}\n")
                log_handle.flush()
        log_handle.close()

    reader_thread = threading.Thread(target=reader, daemon=True)
    reader_thread.start()



class LogFileManager:
    """
    Manager for process log files with automatic creation and cleanup.

    Creates unique log files in /tmp for each process and ensures proper
    cleanup after test completion to prevent file system clutter.
    """

    def __init__(self):
        """Initialise log file manager with empty state."""
        self.log_files = []
        self.file_positions = {}
        self.temp_dir = None

    def create_log_file(self, process_name: str) -> str:
        """
        Create a unique log file for a process.

        Args:
            process_name: Name of the process requiring logging

        Returns:
            Full path to the created log file
        """
        if not self.temp_dir:
            # Create temporary directory for all test log files
            self.temp_dir = tempfile.mkdtemp(prefix="grpc_test_logs_")

        # Generate unique log file path with timestamp
        timestamp = int(time.time())
        log_file = os.path.join(self.temp_dir, f"{process_name}_{timestamp}.log")

        # Create empty log file
        Path(log_file).touch()
        self.log_files.append(log_file)
        self.file_positions[log_file] = 0

        return log_file

    def get_all_log_files(self) -> List[str]:
        """
        Retrieve list of all created log files.

        Returns:
            List of absolute paths to all created log files
        """
        return self.log_files.copy()

    def cleanup(self):
        """Remove all log files and temporary directory."""
        for log_file in self.log_files:
            try:
                if os.path.exists(log_file):
                    os.remove(log_file)
            except Exception as e:
                print(f"Warning: Could not remove log file {log_file}: {e}")

        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                os.rmdir(self.temp_dir)
            except Exception as e:
                print(f"Warning: Could not remove temp directory {self.temp_dir}: {e}")

        self.log_files.clear()
        self.file_positions.clear()
        self.temp_dir = None

    def _scan_content_for_errors(self, content: str) -> List[str]:
        """
        Scan text content for gRPC error patterns.

        Args:
            content: Text content to scan for error patterns

        Returns:
            List of lines containing detected error patterns
        """
        detected_errors = []

        for line in content.split("\n"):
            line = line.strip()
            if not line:
                continue

            # Check each error pattern against the line
            for pattern in GRPC_ERROR_PATTERNS:
                if re.search(pattern, line, re.IGNORECASE):
                    detected_errors.append(line)
                    break  # Avoid duplicate detection of same line

        return detected_errors

    def check_for_errors(self):
        for log_file in self.get_all_log_files():
            if not os.path.exists(log_file):
                continue

            # Read new content since last check
            try:
                with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                    f.seek(self.file_positions[log_file])
                    new_content = f.read()
                    self.file_positions[log_file] = f.tell()
            except (IOError, OSError):
                # File may not be ready yet, continue monitoring
                continue

            if new_content:
                # Check new content for error patterns
                error_lines = self._scan_content_for_errors(new_content)
                if error_lines:
                    # Store error details and signal detection
                    self.detected_error = {
                        "file": log_file,
                        "lines": error_lines,
                    }
                    return self.detected_error
        return None


def run_process_manager_server(
    manager_max_workers: int,
    server_port: int,
    log_file: str,
    server_options: List[Tuple[str, any]] = None,
    ready_event: multiprocessing.Event = None,
    stop_event: multiprocessing.Event = None,
) -> None:
    """
    Run Manager server in a separate process with output logging.

    Args:
        server_port: Port number for the Manager's gRPC server
        log_file: Path to log file for process output
        server_options: List of gRPC server configuration options
        ready_event: Event to signal when server is ready
        stop_event: Event to signal server shutdown request
    """
    stderr_observer(log_file)
    import grpc

    def signal_handler(signum, frame):
        """Handle shutdown signals gracefully."""
        if stop_event:
            stop_event.set()

    # Configure signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    class ManagerServiceImpl(ManagerServiceServicer):
        """Implementation of Manager gRPC service."""

        def MakeRequest(self, request, context):
            """Handle incoming requests to Manager service."""
            return DummyResponse(reply=f"Manager server response: {request.message}")

    # Create and configure gRPC server
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=manager_max_workers),
        options=server_options or [],
    )

    add_ManagerServiceServicer_to_server(ManagerServiceImpl(), server)
    port = server.add_insecure_port(f"[::]:{server_port}")

    try:
        server.start()
        print(f"Manager server started on port {port}")

        # Signal readiness to parent process
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
    controller_max_workers: int,
    server_port: int,
    manager_port: int,
    log_file: str,
    server_options: List[Tuple[str, any]] = None,
    client_options: List[Tuple[str, any]] = None,
    ready_event: multiprocessing.Event = None,
    stop_event: multiprocessing.Event = None,
) -> None:
    """
    Run RootController server with Manager client in a separate process.

    Args:
        server_port: Port number for the RootController's gRPC server
        manager_port: Port number of the Manager server to connect to
        log_file: Path to log file for process output
        server_options: List of gRPC server configuration options
        client_options: List of gRPC client configuration options
        ready_event: Event to signal when server is ready
        stop_event: Event to signal server shutdown request
    """
    stderr_observer(log_file)
    import grpc

    def signal_handler(signum, frame):
        """Handle shutdown signals gracefully."""
        if stop_event:
            stop_event.set()

    # Configure signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    class RootControllerServiceImpl(RootControllerServiceServicer):
        """Implementation of RootController gRPC service."""

        def MakeRequest(self, request, context):
            """Handle incoming requests to RootController service."""
            return DummyResponse(
                reply=f"RootController server response: {request.message}"
            )

    # Create and configure gRPC server
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=controller_max_workers),
        options=server_options or [],
    )

    add_RootControllerServiceServicer_to_server(RootControllerServiceImpl(), server)
    port = server.add_insecure_port(f"[::]:{server_port}")

    # Set up client connection to Manager
    manager_channel = None
    try:
        server.start()

        # Establish connection to Manager server
        manager_channel = grpc.insecure_channel(
            f"localhost:{manager_port}", options=client_options or []
        )
        print(
            f"RootController server started on port {port}, connected to Manager on {manager_port}"
        )

        # Signal readiness to parent process
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
    controller_max_workers: int,
    server_port: int,
    root_port: int,
    child_name: str,
    log_file: str,
    server_options: List[Tuple[str, any]] = None,
    client_options: List[Tuple[str, any]] = None,
    ready_event: multiprocessing.Event = None,
    stop_event: multiprocessing.Event = None,
) -> None:
    """
    Run ChildController server with RootController client in a separate process.

    Args:
        server_port: Port number for the ChildController's gRPC server
        root_port: Port number of the RootController server to connect to
        child_name: Unique identifier for this child controller
        log_file: Path to log file for process output
        server_options: List of gRPC server configuration options
        client_options: List of gRPC client configuration options
        ready_event: Event to signal when server is ready
        stop_event: Event to signal server shutdown request
    """
    stderr_observer(log_file)
    import grpc

    def signal_handler(signum, frame):
        """Handle shutdown signals gracefully."""
        if stop_event:
            stop_event.set()

    # Configure signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    class ChildControllerServiceImpl(ChildControllerServiceServicer):
        """Implementation of ChildController gRPC service."""

        def __init__(self, name: str):
            """Initialise with child controller name."""
            self.name = name

        def MakeRequest(self, request, context):
            """Handle incoming requests to ChildController service."""
            return DummyResponse(
                reply=f"{self.name} server response: {request.message}"
            )

    # Create and configure gRPC server
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=controller_max_workers),
        options=server_options or [],
    )

    add_ChildControllerServiceServicer_to_server(
        ChildControllerServiceImpl(child_name), server
    )
    port = server.add_insecure_port(f"[::]:{server_port}")

    # Set up client connection to RootController
    root_channel = None
    try:
        server.start()

        # Establish connection to RootController server
        root_channel = grpc.insecure_channel(
            f"localhost:{root_port}", options=client_options or []
        )
        print(
            f"{child_name} server started on port {port}, connected to RootController on {root_port}"
        )

        # Signal readiness to parent process
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
    Client for coordinating communication with all gRPC tree components.

    Manages connections to all server processes and provides unified
    interface for testing communication across the entire tree structure.
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


class IndependentRootControllerClient:
    """
    Managed direct gRPC client connection to RootController server.

    Uses file descriptor level stderr redirection to capture gRPC absl logging
    output that bypasses Python's sys.stderr object.
    """

    def __init__(
        self,
        client_id: str,
        root_port: int,
        client_options: List[Tuple[str, any]] = None,
    ):
        """
        Initialise DirectRootClient.

        Args:
            client_id: Unique identifier for this client instance
            root_port: Port number of the RootController server
            client_options: List of gRPC client configuration options
        """
        self.client_id = client_id
        self.root_port = root_port
        self.client_options = client_options or []

        # Connection state
        self.channel = None
        self.stub = None
        self.log_file = None
        self._connected = False

    def _connect_with_stderr_redirect(self, log_file: str):
        """
        Internal method to establish connection with file descriptor level stderr redirection.

        Args:
            log_file: Path to log file for stderr redirection

        Raises:
            RuntimeError: If connection fails
   
        """
        if self._connected:
            return

        self.log_file = log_file
        stderr_observer(log_file)
        import grpc

        try:
            self.channel = grpc.insecure_channel(
                f"localhost:{self.root_port}", options=self.client_options
            )
            self.stub = RootControllerServiceStub(self.channel)
            self._connected = True

        except Exception as e:
            raise RuntimeError(
                f"Failed to establish gRPC connection for {self.client_id}: {e}"
            )

    def make_request(self, message: str) -> DummyResponse:
        """
        Send request to RootController server.

        Args:
            message: Message to include in the request

        Returns:
            Response from the RootController server

        Raises:
            RuntimeError: If not connected
        """
        if not self._connected or not self.stub:
            raise RuntimeError(f"DirectRootClient {self.client_id} is not connected")

        request = DummyRequest(
            message=message,
            timestamp=int(time.time() * 1000),
        )

        return self.stub.MakeRequest(request)

    def _disconnect(self):
        """Internal method to close connection and restore stderr."""
        if self.channel:
            self.channel.close()
            self.channel = None
            self.stub = None

        self._connected = False

    def get_log_file(self) -> str:
        """Get path to log file where client errors are written."""
        return self.log_file

    def is_connected(self) -> bool:
        """Check if client is currently connected."""
        return self._connected

    def get_id(self) -> str:
        """Get unique identifier for this client."""
        return self.client_id


class GrpcProcessTreeManager:
    """
    Context manager for multi-process gRPC tree lifecycle management.

    Provides automatic setup and cleanup of all server processes, log files,
    and monitoring infrastructure.
    support for process manager -> root controller -> child controllers
    and connecticting independent direct clients to the root controller.
    """

    def __init__(
        self,
        number_of_children: int,
        manager_max_workers: int,
        controller_max_workers: int,
        manager_server_config: List[Tuple[str, any]] = None,
        manager_client_config: List[Tuple[str, any]] = None,
        root_server_config: List[Tuple[str, any]] = None,
        root_client_config: List[Tuple[str, any]] = None,
        child_server_config: List[Tuple[str, any]] = None,
        child_client_config: List[Tuple[str, any]] = None,
        env_vars: Dict[str, str] = {}
    ):
        """
        Initialise GrpcTreeManager with configuration for all components.

        Args:
            number_of_children: Number of child controllers to create
            manager_server_config: gRPC options for Manager's server
            manager_client_config: gRPC options for Manager's client
            root_server_config: gRPC options for RootController's server
            root_client_config: gRPC options for RootController's client
            child_server_config: gRPC options for ChildController servers
            child_client_config: gRPC options for ChildController clients
        """
        if number_of_children < 0:
            raise ValueError("Number of children must be non-negative")

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
        self.processes = []
        self.stop_events = []
        self.process_manager = None
        self.log_file_manager = LogFileManager()
        self.direct_clients = {}
        self.env_vars = env_vars

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
        Internal method to create and start all server processes.

        Returns:
            ProcessManagerClient: Client for communicating with all servers
        """
        ready_events = []

        # Create Manager server process
        manager_ready = multiprocessing.Event()
        manager_stop = multiprocessing.Event()
        manager_log = self.log_file_manager.create_log_file("ManagerServer")

        manager_process = multiprocessing.Process(
            target=self._run_with_env,
            args=(
                run_process_manager_server,
                self.manager_max_workers,
                self.manager_port,
                manager_log,
                self.manager_server_config,
                manager_ready,
                manager_stop,
            ),
            name="ManagerServer",
        )

        self.processes.append(manager_process)
        ready_events.append(manager_ready)
        self.stop_events.append(manager_stop)

        # Create RootController server process
        root_ready = multiprocessing.Event()
        root_stop = multiprocessing.Event()
        root_log = self.log_file_manager.create_log_file("RootControllerServer")

        root_process = multiprocessing.Process(
            target=self._run_with_env,
            args=(
                run_root_controller_server,
                self.controller_max_workers,
                self.root_port,
                self.manager_port,
                root_log,
                self.root_server_config,
                self.root_client_config,
                root_ready,
                root_stop,
            ),
            name="RootControllerServer",
        )

        self.processes.append(root_process)
        ready_events.append(root_ready)
        self.stop_events.append(root_stop)

        # Create ChildController server processes
        for i in range(self.number_of_children):
            child_port = self.child_ports[i]
            child_name = f"ChildController{i + 1}"
            child_ready = multiprocessing.Event()
            child_stop = multiprocessing.Event()
            child_log = self.log_file_manager.create_log_file(f"ChildServer{i + 1}")

            child_process = multiprocessing.Process(
                target=self._run_with_env,
                args=(
                    run_child_controller_server,
                    self.controller_max_workers,
                    child_port,
                    self.root_port,
                    child_name,
                    child_log,
                    self.child_server_config,
                    self.child_client_config,
                    child_ready,
                    child_stop,
                ),
                name=f"ChildServer{i + 1}",
            )

            self.processes.append(child_process)
            ready_events.append(child_ready)
            self.stop_events.append(child_stop)

        log_files = self.log_file_manager.get_all_log_files()

        print("Log files:")
        for log_file in log_files:
            print(f"   {log_file}")

        # Start all server processes
        print(f"Starting {len(self.processes)} server processes...")
        for process in self.processes:
            process.start()

        # Wait for all servers to be ready
        print("Waiting for all servers to be ready...")
        for ready_event in ready_events:
            ready_event.wait(timeout=10)

        # Allow extra time for servers to be fully ready
        time.sleep(1)

        # Create and return ProcessManagerClient
        self.process_manager = ProcessManagerClient(
            manager_port=self.manager_port,
            root_port=self.root_port,
            child_ports=self.child_ports,
            client_options=self.manager_client_config,
        )

        return self.process_manager

    def _run_with_env(self, target_func, *args, **kwargs):
        """Wrapper to set environment variables before running target function."""
        import os
        for key, value in self.env_vars.items():
            os.environ[key] = value
        return target_func(*args, **kwargs)

    def _cleanup_tree(self) -> None:
        """Internal method to clean up all resources."""
        if hasattr(self, "direct_clients"):
            for client_id in list(self.direct_clients.keys()):
                self.remove_direct_client(client_id)

        # Close client connections
        if self.process_manager:
            self.process_manager.close_all_connections()

        # Stop all server processes
        self._stop_all_processes()

        # Clean up log files
        self.log_file_manager.cleanup()

    def _stop_all_processes(self):
        """Stop all server processes gracefully with proper cleanup."""
        print("Shutting down all processes...")

        # Signal graceful shutdown to all processes
        for stop_event in self.stop_events:
            stop_event.set()

        # Wait for processes to terminate gracefully
        start_time = time.time()
        timeout = 10

        for process in self.processes:
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

    def check_for_errors(self):
        """
        Check if any gRPC errors have been detected in log files.
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
            DirectRootClient: Ready-to-use direct client connection

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
            DirectRootClient instance

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
