import logging
from typing import Generator

import pytest

from drunc.grpc_testing_tools.grpc_log_file_manager import LogFileManager
from drunc.grpc_testing_tools.grpc_server_manager import GrpcServerManager
from drunc.grpc_testing_tools.multiprocessing_connection_manager import (
    MultiprocessingConnectionManager,
)
from drunc.grpc_testing_tools.port_cleaner import kill_process_on_port
from drunc.processes.ssh_process_lifetime_manager_paramiko import (
    SSHProcessLifetimeManagerParamiko,
)
from drunc.processes.ssh_process_lifetime_manager_shell import (
    SSHProcessLifetimeManagerShell,
)


class GprcProcessManagerServiceResources:
    """Container for gRPC test resources requiring cleanup."""

    def __init__(self):
        self.log_manager: LogFileManager | None = None
        self.connection_manager: MultiprocessingConnectionManager | None = None
        self.server_manager: GrpcServerManager | None = None
        self.manager_channel = None
        self.root_channel = None
        self.ports_to_cleanup: list[int] = []


@pytest.fixture
def grpc_process_manager_service_resources() -> Generator[
    GprcProcessManagerServiceResources, None, None
]:
    """
    Fixture providing gRPC test resources with guaranteed cleanup.

    Ensures all resources are cleaned up even if the test fails,
    preventing orphaned processes and open channels. Also forcefully
    kills any processes still bound to test ports.

    Yields:
        GprcProcessManagerServiceResources: Container for gRPC test resources
    """
    resources = GprcProcessManagerServiceResources()

    try:
        yield resources
    finally:
        # Guaranteed cleanup in reverse order of creation
        print("\n=== Fixture Cleanup ===")

        # Close gRPC channels
        if resources.root_channel:
            try:
                resources.root_channel.close()
                print("RootController channel closed")
            except Exception as e:
                print(f"Warning: Error closing root channel: {e}")

        if resources.manager_channel:
            try:
                resources.manager_channel.close()
                print("Manager channel closed")
            except Exception as e:
                print(f"Warning: Error closing manager channel: {e}")

        # Clean up server manager (stops all servers)
        if resources.server_manager:
            try:
                resources.server_manager.cleanup()
                print("Server manager cleanup completed")
            except Exception as e:
                print(f"Warning: Error during server cleanup: {e}")

        # Clean up connection manager
        if resources.connection_manager:
            try:
                resources.connection_manager.cleanup()
                print("Connection manager cleanup completed")
            except Exception as e:
                print(f"Warning: Error during connection cleanup: {e}")

        # Force kill any remaining processes on test ports
        if resources.ports_to_cleanup:
            print("\n=== Force cleanup of test ports ===")
            for port in resources.ports_to_cleanup:
                killed = kill_process_on_port(port)
                if killed:
                    print(f"Cleaned up orphaned process on port {port}")

        # Clean up log manager
        if resources.log_manager:
            try:
                resources.log_manager.cleanup()
                print("Log manager cleanup completed")
            except Exception as e:
                print(f"Warning: Error during log cleanup: {e}")


@pytest.fixture
def ssh_manager_paramiko() -> Generator[SSHProcessLifetimeManagerParamiko, None, None]:
    """
    Fixture providing Paramiko-based SSH manager with cleanup.

    Yields:
        SSHProcessLifetimeManagerParamiko: Configured manager with logging
    """
    # allows debug logging without spamming paramiko logs
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    # set up logger for our SSH manager
    logger = logging.getLogger("test_ssh_paramiko")
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(console_handler)

    manager = SSHProcessLifetimeManagerParamiko(
        disable_localhost_host_key_check=True,
        disable_host_key_check=False,
        logger=logger,
    )

    yield manager

    manager.kill_all_processes()


@pytest.fixture
def ssh_manager_shell() -> Generator[SSHProcessLifetimeManagerShell, None, None]:
    """
    Fixture providing shell-based SSH manager with cleanup.

    Yields:
        SSHProcessLifetimeManagerShell: Configured manager with logging
    """
    logging.getLogger("sh.command").setLevel(logging.WARNING)
    logging.getLogger("sh.stream_bufferer").setLevel(logging.WARNING)
    logging.getLogger("sh.streamreader").setLevel(logging.WARNING)
    logger = logging.getLogger("test_ssh_shell")
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(console_handler)

    manager = SSHProcessLifetimeManagerShell(
        disable_localhost_host_key_check=True,
        disable_host_key_check=False,
        logger=logger,
    )

    yield manager

    manager.kill_all_processes()
