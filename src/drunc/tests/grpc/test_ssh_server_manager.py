"""
Tests for SSH-based Manager server lifecycle management
"""

import os
import time

import grpc
import pytest

from drunc.tests.grpc.available_grpc_servers import ServerType
from drunc.tests.grpc.grpc_log_file_manager import LogFileManager
from drunc.tests.grpc.grpc_server_manager import GrpcServerConfig
from drunc.tests.grpc.remote_cli_command_builder import RemoteCLICommandBuilder

# Import the updated SSH managers
from drunc.tests.grpc.ssh_connection_manager import SSHConnectionManager
from drunc.tests.grpc.ssh_server_manager import SSHGrpcServerManager

# Import gRPC generated code for client communication
from drunc.tests.grpc.test_pb2 import DummyRequest, KillRequest
from drunc.tests.grpc.test_pb2_grpc import ManagerServiceStub


def test_ssh_manager_server_lifecycle_with_env():
    """
    Test complete SSH Manager server lifecycle using clean architecture.

    This test verifies that:
    1. SSH Manager server can be started with pre-built boot commands
    2. gRPC requests can be made to the server
    3. Kill request properly shuts down the server
    4. Process cleanup works correctly
    """

    env_script_dir = "/home/aurash/work/09sept"
    env_file = "env.sh"

    # Environment script configuration
    env_setup_script = f"cd {env_script_dir} && source {env_file}"

    assert os.path.exists(env_script_dir), (
        f"Environment script directory not found: {env_script_dir}"
    )
    assert os.path.exists(os.path.join(env_script_dir, env_file)), (
        f"Environment script file not found: {env_file}"
    )

    # Test configuration
    server_port = 50080
    max_workers = 2
    server_timeout = 30.0

    print("=== SSH Manager Server Lifecycle Test (Clean Architecture) ===")
    print(f"Environment script: {env_setup_script}")
    print(f"Server port: {server_port}")
    print(f"Max workers: {max_workers}")
    print(f"Timeout: {server_timeout}s")
    print(f"Current user: {os.getenv('USER', 'unknown')}")
    print(f"Current directory: {os.getcwd()}")

    # Create log file manager for output capture
    log_manager = LogFileManager()
    log_file = log_manager.create_log_file("SSHManagerTest")
    print(f"Test log file: {log_file}")

    command_builder = RemoteCLICommandBuilder(
        env_setup_script=env_setup_script,
        python_executable="python3",
        working_directory=None,  # Use current directory
        default_user=os.getenv("USER"),
        hosts=["localhost"],
        disable_host_key_check=True,
        ssh_options=[],
        env_vars={"GRPC_TRACE": "http"},
    )

    manager_config = GrpcServerConfig(
        server_id="TestManagerServerClean",
        server_type=ServerType.MANAGER,
        host="localhost",
        port=server_port,
        max_workers=max_workers,
        log_file=log_file,
        server_options=[],
        client_options=[],
    )

    # Create SSH connection manager with pre-built boot command
    ssh_connection_manager = SSHConnectionManager(
        command_builder=command_builder,
        boot_command_configs={"TestManagerServerClean": manager_config},
        log_directory=None,  # Use temporary directory
    )

    # Create SSH server manager
    ssh_server_manager = SSHGrpcServerManager(connection_manager=ssh_connection_manager)

    server_handle = None
    channel = None

    print("\n=== Starting SSH Manager Server ===")

    # Start the Manager server via SSH
    server_handle = ssh_server_manager.start_manager_server(manager_config)
    assert server_handle is not None, "Failed to create server handle"
    print(f"Server handle created: {server_handle.process_id}")

    # TODO refactor wait_for_server_ready to use grpc
    # Wait for server to be ready with detailed feedback
    print("\n=== Waiting for Server Ready ===")
    ready = ssh_server_manager.wait_for_server_ready(
        "TestManagerServerClean", timeout=server_timeout
    )

    if not ready:
        # Get detailed error information
        startup_error = ssh_connection_manager.get_process_startup_error(server_handle)
        error_details = (
            f"Server readiness check failed. Startup error: {startup_error or 'None'}"
        )

        # Try to read log file for additional context
        try:
            with open(log_file, "r") as f:
                log_content = f.read()
                if log_content.strip():
                    error_details += f"\n\nServer log content:\n{log_content}"
                else:
                    error_details += "\n\nServer log file is empty"
        except Exception as log_error:
            error_details += f"\n\nCould not read server log: {log_error}"

        pytest.fail(error_details)

    if not ssh_connection_manager.is_process_alive(server_handle):
        pytest.fail("Server process is not alive after startup")

    print("✓ Server is ready")

    # Create gRPC client connection for testing
    print("\n=== Testing gRPC Communication ===")
    channel = grpc.insecure_channel(f"localhost:{server_port}")
    stub = ManagerServiceStub(channel)

    # Test basic connectivity with Manager server
    request = DummyRequest(
        message="Test request from SSH clean architecture test client",
        timestamp=int(time.time() * 1000),
    )

    try:
        response = stub.MakeRequest(request)
        assert "Manager server response" in response.reply, (
            f"Unexpected response: {response.reply}"
        )
        print(f"✓ Received response: {response.reply}")
    except grpc.RpcError as e:
        error_msg = f"Failed to communicate with Manager server: {e}"

        # Add context from server logs if available
        try:
            with open(log_file, "r") as f:
                log_content = f.read()
                if log_content.strip():
                    error_msg += f"\n\nServer log content:\n{log_content}"
        except Exception:
            pass

        pytest.fail(error_msg)

    # Test graceful shutdown using Kill request
    print("\n=== Testing Graceful Shutdown ===")
    kill_request = KillRequest(
        reason="SSH clean architecture test completion",
        grace_period_seconds=3,
    )

    print("Sending Kill request...")
    kill_response = stub.Kill(kill_request)
    assert kill_response.shutdown_initiated, "Kill request should initiate shutdown"
    print(f"✓ Kill response: {kill_response.message}")

    # Wait for server process to terminate gracefully
    print("\n=== Waiting for Server Shutdown ===")
    # Verify server is no longer running
    shutdown_timeout = 10.0
    start_time = time.time()

    while (
        time.time() - start_time
    ) < shutdown_timeout and ssh_server_manager.is_server_running(
        "TestManagerServerClean"
    ):
        time.sleep(0.5)

    if ssh_server_manager.is_server_running("TestManagerServerClean"):
        pytest.fail("Server did not shut down within the expected time")
    else:
        elapsed = time.time() - start_time
        print(f"✓ Server reports as stopped after {elapsed:.1f}s")

    start_time = time.time()

    while (
        time.time() - start_time
    ) < shutdown_timeout and ssh_connection_manager.is_process_alive(server_handle):
        time.sleep(0.5)

    if ssh_connection_manager.is_process_alive(server_handle):
        pytest.fail("Server did not shut down within the expected time")
    else:
        elapsed = time.time() - start_time
        print(f"✓ Server process terminated after {elapsed:.1f}s")

    print("✓ Server reports as stopped")

    print("\n✓ SSH Manager server test with clean architecture completed successfully")

    # Clean up all resources with detailed logging
    print("\n=== Cleanup ===")

    # Clean up server manager (includes stopping servers)
    try:
        ssh_server_manager.cleanup()
        print("✓ Server manager cleanup completed")
    except Exception as e:
        print(f"Warning: Error during server cleanup: {e}")

    # Clean up log manager
    try:
        log_manager.cleanup()
        print("✓ Log manager cleanup completed")
    except Exception as e:
        print(f"Warning: Error during log cleanup: {e}")


if __name__ == "__main__":
    """
    Run the test directly for debugging purposes.
    
    This allows the test to be executed standalone for development
    and debugging without requiring pytest.
    """

    try:
        test_ssh_manager_server_lifecycle_with_env()
        print("SSH Manager test with clean architecture passed!")
    except Exception as e:
        print(f"SSH Manager test with clean architecture failed: {e}")
        raise
