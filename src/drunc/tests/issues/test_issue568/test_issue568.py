#!/usr/bin/env python3
"""
Multi-Process gRPC Tree Structure Tests with Log File Monitoring

This module provides comprehensive testing of gRPC multi-process architectures
with real-time error detection. Each process logs to individual files in /tmp,
and tests fail immediately when gRPC errors such as ping_timeout are detected.

Architecture:
- Test Process: Coordinates testing and monitors log files
- Manager Server: Central coordination service
- RootController Server: Primary controller with Manager client
- ChildController Servers: Leaf nodes with RootController clients

All inter-component communication occurs via gRPC across process boundaries.
Error detection monitors all process log files in real-time for immediate failure.
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

import grpc
import pytest

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

# gRPC error patterns to detect in log files
GRPC_ERROR_PATTERNS = [
    "ping_timeout",
    "keepalive.*timeout",
    "chttp2_transport.*GOAWAY",
    "GOAWAY.*UNAVAILABLE",
    "Error code.*ping_timeout",
    "grpc.*UNAVAILABLE.*ping",
]


class LogFileManager:
    """
    Manager for process log files with automatic creation and cleanup.

    Creates unique log files in /tmp for each process and ensures proper
    cleanup after test completion to prevent file system clutter.
    """

    def __init__(self):
        """Initialise log file manager with empty state."""
        self.log_files = []
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
        self.temp_dir = None


class LogFileMonitor:
    """
    Real-time monitor for multiple log files that detects gRPC error patterns.

    Monitors process log files continuously in a background thread and triggers
    immediate test failure when error patterns are detected. Provides fast-fail
    behaviour for efficient testing.
    """

    def __init__(self, log_files: List[str]):
        """
        Initialise monitor with list of log files to watch.

        Args:
            log_files: List of absolute paths to log files for monitoring
        """
        self.log_files = log_files
        self.monitoring = False
        self.monitor_thread = None
        self.stop_event = threading.Event()
        self.error_detected_event = threading.Event()
        self.detected_error = None
        self.file_positions = {}

    def start_monitoring(self):
        """Start monitoring log files in background thread."""
        if self.monitoring:
            return

        self.monitoring = True
        self.stop_event.clear()
        self.error_detected_event.clear()

        # Initialise file positions to track reading progress
        for log_file in self.log_files:
            self.file_positions[log_file] = 0

        # Start background monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()

    def stop_monitoring(self):
        """Stop monitoring and cleanup background thread."""
        if not self.monitoring:
            return

        self.monitoring = False
        self.stop_event.set()

        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=2)

    def check_for_errors(self):
        """
        Check if any errors have been detected

        """
        if self.error_detected_event.is_set() and self.detected_error != None:
            return self.detected_error

        return None

    def _monitor_loop(self):
        """
        Main monitoring loop running in background thread.

        Continuously checks all log files for new content and scans for
        error patterns. Sets error detection event when errors are found.
        """
        while not self.stop_event.is_set():
            try:
                # Check each log file for new content
                for log_file in self.log_files:
                    if not os.path.exists(log_file):
                        continue

                    # Read new content since last check
                    try:
                        with open(
                            log_file, "r", encoding="utf-8", errors="ignore"
                        ) as f:
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
                            self.error_detected_event.set()
                            return

                # Brief pause before next monitoring cycle
                self.stop_event.wait(0.5)

            except Exception as e:
                # Log monitoring errors but continue monitoring
                print(f"Log monitoring error: {e}")
                self.stop_event.wait(1)

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
            print(f"Scanning line: {line}")

            # Check each error pattern against the line
            for pattern in GRPC_ERROR_PATTERNS:
                if re.search(pattern, line, re.IGNORECASE):
                    detected_errors.append(line)
                    break  # Avoid duplicate detection of same line

        return detected_errors


class InterruptibleWait:
    """
    Interruptible waiting mechanism with status updates.

    Provides cancellable waiting with periodic progress updates as an
    alternative to time.sleep() that can be interrupted gracefully.
    """

    def __init__(self):
        """Initialise interruptible wait with default state."""
        self.cancelled = threading.Event()

    def wait(
        self, duration: int, status_prefix: str = "Waiting", update_interval: int = 30
    ) -> bool:
        """
        Wait for specified duration with periodic status updates.

        Args:
            duration: Total time to wait in seconds
            status_prefix: Prefix for status update messages
            update_interval: Seconds between status updates

        Returns:
            True if wait completed normally, False if cancelled
        """
        start_time = time.time()

        while not self.cancelled.is_set():
            elapsed = int(time.time() - start_time)

            if elapsed >= duration:
                print(f"{status_prefix} completed after {elapsed} seconds")
                return True

            remaining = duration - elapsed
            print(
                f"   {status_prefix}: {elapsed}/{duration} seconds elapsed, {remaining} seconds remaining"
            )

            # Wait for update interval or cancellation signal
            if self.cancelled.wait(timeout=update_interval):
                print(f"{status_prefix} cancelled after {elapsed} seconds")
                return False

        return False

    def cancel(self):
        """Cancel the wait operation."""
        self.cancelled.set()


def redirect_process_output_to_file(log_file: str):
    """
    Redirect stdout and stderr of current process to specified log file.

    This function should be called at the start of each server process
    to ensure all output (including gRPC logs) is written to the log file.

    Args:
        log_file: Absolute path to log file for output redirection
    """
    # Ensure any pending output is flushed
    sys.stdout.flush()
    sys.stderr.flush()

    # Open log file for writing (create if doesn't exist, truncate if exists)
    log_fd = os.open(log_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)

    # Redirect stdout and stderr to log file
    os.dup2(log_fd, sys.stdout.fileno())
    os.dup2(log_fd, sys.stderr.fileno())

    # Close original file descriptor
    os.close(log_fd)

    # Reopen stdout and stderr as unbuffered for real-time logging
    sys.stdout = os.fdopen(sys.stdout.fileno(), "w", 1)
    sys.stderr = os.fdopen(sys.stderr.fileno(), "w", 1)


def run_manager_server(
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
    # Redirect all output to log file
    redirect_process_output_to_file(log_file)

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
        futures.ThreadPoolExecutor(max_workers=MANAGER_MAX_WORKERS),
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
    # Redirect all output to log file
    redirect_process_output_to_file(log_file)

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
        futures.ThreadPoolExecutor(max_workers=CONTROLLER_MAX_WORKERS),
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
    # Redirect all output to log file
    redirect_process_output_to_file(log_file)

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
        futures.ThreadPoolExecutor(max_workers=CONTROLLER_MAX_WORKERS),
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


class GrpcTreeManager:
    """
    Context manager for multi-process gRPC tree lifecycle management.

    Provides automatic setup and cleanup of all server processes, log files,
    and monitoring infrastructure. Ensures proper resource management and
    graceful shutdown with real-time error detection.
    """

    def __init__(
        self,
        number_of_children: int,
        manager_server_config: List[Tuple[str, any]] = None,
        manager_client_config: List[Tuple[str, any]] = None,
        root_server_config: List[Tuple[str, any]] = None,
        root_client_config: List[Tuple[str, any]] = None,
        child_server_config: List[Tuple[str, any]] = None,
        child_client_config: List[Tuple[str, any]] = None,
        monitor_errors: bool = True,
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
            monitor_errors: Whether to monitor log files for gRPC errors
        """
        if number_of_children < 0:
            raise ValueError("Number of children must be non-negative")

        self.number_of_children = number_of_children
        self.manager_server_config = manager_server_config or []
        self.manager_client_config = manager_client_config or []
        self.root_server_config = root_server_config or []
        self.root_client_config = root_client_config or []
        self.child_server_config = child_server_config or []
        self.child_client_config = child_client_config or []
        self.monitor_errors = monitor_errors

        # Calculate port assignments
        self.manager_port = BASE_MANAGER_PORT
        self.root_port = BASE_ROOT_PORT
        self.child_ports = [BASE_CHILD_PORT + i for i in range(number_of_children)]

        # Runtime state
        self.processes = []
        self.stop_events = []
        self.process_manager = None
        self.log_file_manager = LogFileManager()
        self.log_monitor = None

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
            target=run_manager_server,
            args=(
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
            target=run_root_controller_server,
            args=(
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
                target=run_child_controller_server,
                args=(
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

        # Start all server processes
        print(f"Starting {len(self.processes)} server processes...")
        for process in self.processes:
            process.start()

        # Start log file monitoring if enabled
        if self.monitor_errors:
            log_files = self.log_file_manager.get_all_log_files()
            self.log_monitor = LogFileMonitor(log_files)
            self.log_monitor.start_monitoring()
            print(f"Started monitoring {len(log_files)} log files for gRPC errors")

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

    def _cleanup_tree(self) -> None:
        """Internal method to clean up all resources."""
        # Stop log file monitoring
        if self.log_monitor:
            self.log_monitor.stop_monitoring()

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
        if self.log_monitor and self.monitor_errors:
            return self.log_monitor.check_for_errors()

    def get_root_port(self) -> int:
        """
        Get the port number of the RootController server.

        Returns:
            Port number where RootController is listening
        """
        return self.root_port

    def create_direct_root_client(
        self, client_options: List[Tuple[str, any]] = None
    ) -> RootControllerServiceStub:
        """
        Create a direct gRPC client connection to the RootController server.

        This creates an independent client connection separate from the
        ProcessManagerClient for testing scenarios with multiple clients.

        Args:
            client_options: gRPC client configuration options

        Returns:
            Direct gRPC client stub for RootController

        Note:
            The returned stub must be manually closed via channel.close()
        """
        channel = grpc.insecure_channel(
            f"localhost:{self.root_port}",
            options=client_options or self.root_client_config,
        )
        stub = RootControllerServiceStub(channel)

        # Store channel reference for cleanup
        stub._channel = channel

        return stub


def enable_verbose_logging():
    """Enable verbose gRPC logging for debugging purposes."""
    os.environ["GRPC_VERBOSITY"] = "DEBUG"
    os.environ["GRPC_TRACE"] = "http"


def test_basic_grpc_tree_communication():
    """
    Basic test to verify gRPC tree setup and communication.

    Simple sanity test that sets up minimal tree structure and verifies
    all components can communicate without errors. Does not perform
    extended idle periods or stress testing.
    """
    basic_config = [
        ("grpc.keepalive_time_ms", 60000),  # Standard 60 second keepalive
        ("grpc.keepalive_timeout_ms", 10000),  # 10 second timeout
    ]

    tree_manager = GrpcTreeManager(
        number_of_children=2,
        manager_server_config=basic_config,
        manager_client_config=basic_config,
        root_server_config=basic_config,
        root_client_config=basic_config,
        child_server_config=basic_config,
        child_client_config=basic_config,
        monitor_errors=True,
    )
    with tree_manager as process_manager:
        print("Establishing connections to all servers...")
        process_manager.connect_to_all_servers()

        print("Performing basic communication test...")
        process_manager.perform_full_communication_test()

        print("Creating direct RootController client...")
        direct_root_client = tree_manager.create_direct_root_client()

        # Test direct client communication
        request = DummyRequest(
            message="Basic test from direct client",
            timestamp=int(time.time() * 1000),
        )
        response = direct_root_client.MakeRequest(request)
        print(f"Direct client response: {response.reply}")

        # Clean up direct client connection
        direct_root_client._channel.close()

        print("Basic communication test completed successfully")


def test_multiprocess_http2_ping_timeout():
    """
    Test HTTP/2 ping timeout detection using multi-process architecture.

    Creates scenario with aggressive keepalive settings to trigger ping
    timeout behaviour. Test FAILS IMMEDIATELY if ping_timeout errors
    are detected in any process log files.
    """

    # Aggressive keepalive settings to trigger ping timeout scenarios
    aggressive_config = [
        ("grpc.keepalive_time_ms", 10),
    ]

    tree_manager = GrpcTreeManager(
        number_of_children=5,
        manager_server_config=aggressive_config,
        manager_client_config=aggressive_config,
        root_server_config=aggressive_config,
        root_client_config=aggressive_config,
        child_server_config=aggressive_config,
        child_client_config=aggressive_config,
        monitor_errors=True,
    )

    with tree_manager as process_manager:
        print("Connecting ProcessManagerClient to all servers...")
        process_manager.connect_to_all_servers()

        # Perform initial communication test
        process_manager.perform_full_communication_test()

        # Monitor for ping timeout with periodic error checking
        total_duration = 150
        check_interval = 10  # Check for errors every 10 seconds

        start_time = time.time()
        while (time.time() - start_time) < total_duration:
            # Check for errors in log files
            error_found = tree_manager.check_for_errors()
            if error_found != None:
                # TODO why does this raise an exception?
                pytest.fail(
                    f"gRPC errors detected during ping timeout test: {error_found}"
                )

            elapsed = int(time.time() - start_time)
            remaining = total_duration - elapsed
            print(
                f"   Monitoring for ping timeout behaviour: {elapsed}/{total_duration} seconds elapsed, {remaining} seconds remaining"
            )

            # Brief pause before next check
            time.sleep(check_interval)

        print("Testing communication after monitoring period...")
        process_manager.perform_full_communication_test()
        print("Multi-process ping timeout test completed")


def test_communication_after_root_controller_left_idle():
    """
    Test direct RootController client connection with extended idle period.

    Sets up full gRPC tree, creates separate direct client connection to
    RootController, goes idle for two minutes to observe keepalive behaviour,
    then tests communication stability. FAILS IMMEDIATELY if gRPC errors detected.
    """

    # Configuration for keepalive testing
    keepalive_config = [
        ("grpc.keepalive_time_ms", 30000),  # Send keepalive every 30 seconds
        (
            "grpc.keepalive_timeout_ms",
            5000,
        ),  # Wait 5 seconds for keepalive response
        (
            "grpc.keepalive_permit_without_calls",
            1,
        ),  # Allow keepalive without active calls
    ]

    tree_manager = GrpcTreeManager(
        number_of_children=3,
        manager_server_config=keepalive_config,
        manager_client_config=keepalive_config,
        root_server_config=keepalive_config,
        root_client_config=keepalive_config,
        child_server_config=keepalive_config,
        child_client_config=keepalive_config,
        monitor_errors=True,
    )

    with tree_manager as process_manager:
        print("Setting up full gRPC tree...")
        process_manager.connect_to_all_servers()

        # Verify tree is working with initial communication test
        print("Verifying initial tree communication...")
        process_manager.perform_full_communication_test()

        # Create direct RootController client
        print(
            f"Creating direct RootController client connection to port {tree_manager.get_root_port()}..."
        )
        direct_root_client = tree_manager.create_direct_root_client(
            client_options=keepalive_config
        )

        # Test initial communication with direct client
        print("Testing initial direct client communication...")
        initial_request = DummyRequest(
            message="Initial test from direct RootController client",
            timestamp=int(time.time() * 1000),
        )

        try:
            initial_response = direct_root_client.MakeRequest(initial_request)
            print(f"   Initial Response: {initial_response.reply}")
        except Exception as e:
            print(f"   Initial communication failed: {e}")
            raise

        # Go idle for two minutes with periodic error checking
        total_duration = 120  # 2 minutes
        check_interval = 10  # Check for errors every 10 seconds

        start_time = time.time()
        while (time.time() - start_time) < total_duration:
            # Check for errors in log files
            error_found = tree_manager.check_for_errors()
            if error_found != None:
                pytest.fail(f"gRPC errors detected during idle test: {error_found}")

            elapsed = int(time.time() - start_time)
            remaining = total_duration - elapsed
            print(
                f"   Idle period - monitoring keepalive behaviour: {elapsed}/{total_duration} seconds elapsed, {remaining} seconds remaining"
            )

            # Brief pause before next check
            time.sleep(check_interval)

        print("Testing direct client communication after idle period...")
        post_idle_request = DummyRequest(
            message="Post-idle test from direct RootController client",
            timestamp=int(time.time() * 1000),
        )

        try:
            post_idle_response = direct_root_client.MakeRequest(post_idle_request)
            print(f"   Post-idle Response: {post_idle_response.reply}")
            print("   Direct client connection remained stable through idle period")
        except Exception as e:
            print(f"   Post-idle communication failed: {e}")
            print("   Connection may have been affected by keepalive timeout")

        # Test that the main tree is still functional
        print("Verifying main tree communication after idle period...")
        try:
            process_manager.perform_full_communication_test()
            print("   Main tree communication still functional")
        except Exception as e:
            print(f"   Main tree communication failed: {e}")

        # Clean up direct client connection
        try:
            direct_root_client._channel.close()
            print("Direct client connection closed successfully")
        except Exception as e:
            print(f"Error closing direct client connection: {e}")

        print("Direct RootController client idle test completed")


if __name__ == "__main__":
    """
    Execute complete gRPC multi-process test suite with log file monitoring.
    
    Tests will fail immediately when ping_timeout or other gRPC errors are
    detected in process log files. Use Ctrl+C to interrupt tests gracefully.
    """

    # Uncomment to enable verbose gRPC logging for debugging
    # enable_verbose_logging()

    print("Starting gRPC multi-process test suite with log file monitoring...")

    try:
        # Basic functionality test without error monitoring
        test_basic_grpc_tree_communication()

        # Stress tests with real-time error monitoring
        test_multiprocess_http2_ping_timeout()
        test_communication_after_root_controller_left_idle()

        print(f"\n{'=' * 60}")
        print("All tests completed successfully!")
        print(f"{'=' * 60}")

    except KeyboardInterrupt:
        print("\nTest suite interrupted by user")
        sys.exit(130)

    except Exception as e:
        print(f"\nTest suite failed with error: {e}")
        sys.exit(1)
