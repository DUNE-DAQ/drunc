"""
Test for gRPC server kill functionality using multiprocessing.

This test verifies that the Kill gRPC method properly terminates the server process
and that subsequent requests fail as expected.
"""

import os
import time

import pytest
from grpc import RpcError, StatusCode, insecure_channel

from drunc.tests.grpc.available_grpc_servers import ServerType
from drunc.tests.grpc.grpc_log_file_manager import LogFileManager
from drunc.tests.grpc.grpc_server_manager import GrpcServerConfig, GrpcServerManager
from drunc.tests.grpc.multiprocessing_connection_manager import (
    MultiprocessingConnectionManager,
)
from drunc.tests.grpc.remote_cli_command_builder import RemoteCLICommandBuilder
from drunc.tests.grpc.ssh_connection_manager import SSHConnectionManager

# Import gRPC generated code
from drunc.tests.grpc.test_pb2 import DummyRequest, KillRequest
from drunc.tests.grpc.test_pb2_grpc import ManagerServiceStub


def test_kill_multiprocessing_server_via_grpc():
    """
    Test that verifies proper server termination via gRPC Kill method.

    This test:
    1. Starts a Manager server using multiprocessing
    2. Sends a successful request to verify server is running
    3. Sends a Kill request and verifies successful response
    4. Waits for server process to terminate
    5. Attempts another Kill request and expects gRPC exception
    """

    # Test configuration
    server_port = 50081
    max_workers = 2
    server_timeout = 15.0

    print("=== Test Kill Server via gRPC ===")
    print(f"Server port: {server_port}")
    print(f"Max workers: {max_workers}")
    print(f"Timeout: {server_timeout}s")

    # Create log file manager
    log_manager = LogFileManager()
    log_file = log_manager.create_log_file("TestKillServer")
    print(f"Log file: {log_file}")

    # Create connection and server managers
    connection_manager = MultiprocessingConnectionManager(
        env_vars={"GRPC_TRACE": "http"}
    )
    server_manager = GrpcServerManager(connection_manager)

    # Configure Manager server
    manager_config = GrpcServerConfig(
        server_id="TestKillServer",
        server_type=ServerType.MANAGER,
        host="localhost",
        port=server_port,
        max_workers=max_workers,
        log_file=log_file,
        server_options=[],
        client_options=[],
    )

    server_handle = None
    channel = None

    try:
        print("\n=== Starting Manager Server ===")

        # Start the Manager server
        server_handle = server_manager.start_manager_server(manager_config)
        assert server_handle is not None, "Failed to create server handle"
        print(f"Server handle created: {server_handle.process_id}")

        # Wait for server to be ready
        print("\n=== Waiting for Server Ready ===")
        ready = server_manager.wait_for_server_ready(
            "TestKillServer", timeout=server_timeout
        )

        assert ready, "Server failed to become ready within timeout"
        assert connection_manager.is_process_alive(server_handle), (
            "Server process should be alive"
        )
        print("Server is ready and process is alive")

        # Create gRPC client connection
        print("\n=== Testing Initial gRPC Communication ===")
        channel = insecure_channel(f"localhost:{server_port}")
        stub = ManagerServiceStub(channel)

        # Send initial test request to verify server is working
        initial_request = DummyRequest(
            message="Pre-kill test request",
            timestamp=int(time.time() * 1000),
        )

        initial_response = stub.MakeRequest(initial_request)
        assert "Manager server response" in initial_response.reply
        print(f"Initial request successful: {initial_response.reply}")

        # Send Kill request
        print("\n=== Sending Kill Request ===")
        kill_request = KillRequest(
            reason="Test server kill functionality",
            grace_period_seconds=2,
        )

        # This should succeed and return a response
        kill_response = stub.Kill(kill_request)
        assert kill_response.shutdown_initiated, "Kill request should initiate shutdown"
        print(f"Kill request successful: {kill_response.message}")

        # Wait for server process to actually terminate
        print("\n=== Waiting for Server Process Termination ===")
        termination_timeout = 10.0
        start_time = time.time()

        while (
            time.time() - start_time
        ) < termination_timeout and connection_manager.is_process_alive(server_handle):
            time.sleep(0.5)
            print(".", end="", flush=True)

        print()  # New line after dots

        if connection_manager.is_process_alive(server_handle):
            pytest.fail("Server process did not terminate within expected time")

        elapsed = time.time() - start_time
        print(f"Server process terminated after {elapsed:.1f}s")

        print("\n=== Testing Second Kill Request (Should Fail) ===")
        second_kill_request = KillRequest(
            reason="Second kill attempt - should fail",
            grace_period_seconds=1,
        )

        # we expect this to raise a gRPC exception as server is down
        with pytest.raises(RpcError) as exc_info:
            stub.Kill(second_kill_request)

        # Verify it's the right type of error
        rpc_error = exc_info.value
        assert rpc_error.code() in [
            StatusCode.UNAVAILABLE,
            StatusCode.CANCELLED,
            StatusCode.DEADLINE_EXCEEDED,
        ], f"Expected connection error, got: {rpc_error.code()}"

        print(f"Second Kill request properly failed with: {rpc_error.code()}")
        print("Test passed: Second request failed as expected")

    except RpcError as e:
        if "Second Kill request" in str(e):
            # This is expected for the second kill request
            print(f"Expected gRPC error on second kill: {e}")
        else:
            pytest.fail(f"Unexpected gRPC error: {e}")

    except Exception as e:
        pytest.fail(f"Test failed with unexpected error: {e}")

    finally:
        print("\n=== Cleanup ===")

        # Close gRPC channel
        if channel:
            try:
                channel.close()
                print("gRPC channel closed")
            except Exception as e:
                print(f"Warning: Error closing channel: {e}")

        # Clean up server manager (should handle any remaining processes)
        try:
            server_manager.cleanup()
            print("Server manager cleanup completed")
        except Exception as e:
            print(f"Warning: Error during server cleanup: {e}")

        # Clean up connection manager
        try:
            connection_manager.cleanup()
            print("Connection manager cleanup completed")
        except Exception as e:
            print(f"Warning: Error during connection cleanup: {e}")

    print("\nTest kill_server_via_grpc completed successfully")


def test_kill_ssh_server_via_grpc():
    """
    Test that verifies proper SSH server termination via gRPC Kill method.

    This test:
    1. Starts a Manager server using SSH
    2. Sends a successful request to verify server is running
    3. Sends a Kill request and verifies successful response
    4. Waits for server process to terminate
    5. Attempts another Kill request and expects gRPC exception
    """

    # Environment configuration
    env_script_dir = "/home/aurash/work/09sept"
    env_file = "env.sh"
    env_setup_script = f"cd {env_script_dir} && source {env_file}"

    # Verify environment setup exists
    assert os.path.exists(env_script_dir), (
        f"Environment script directory not found: {env_script_dir}"
    )
    assert os.path.exists(os.path.join(env_script_dir, env_file)), (
        f"Environment script file not found: {env_file}"
    )

    # Test configuration
    server_port = 50082
    max_workers = 2
    server_timeout = 30.0

    print("=== Test Kill SSH Server via gRPC ===")
    print(f"Environment script: {env_setup_script}")
    print(f"Server port: {server_port}")
    print(f"Max workers: {max_workers}")
    print(f"Timeout: {server_timeout}s")

    # Create log file manager
    log_manager = LogFileManager()
    log_file = log_manager.create_log_file("TestManagerServerID")
    print(f"Log file: {log_file}")

    # Create command builder
    command_builder = RemoteCLICommandBuilder(
        env_setup_script=env_setup_script,
        python_executable="python3",
        working_directory=None,
        default_user=os.getenv("USER"),
        hosts=["localhost"],
        disable_host_key_check=True,
        ssh_options=[],
        env_vars={"GRPC_TRACE": "http"},
    )

    # Configure Manager server
    manager_config = GrpcServerConfig(
        server_id="TestManagerServerID",
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
        boot_command_configs={"TestManagerServerID": manager_config},
        log_directory=None,
    )

    # Create SSH server manager
    ssh_server_manager = GrpcServerManager(connection_manager=ssh_connection_manager)

    server_handle = None
    channel = None

    try:
        print("\n=== Starting SSH Manager Server ===")

        # Start the Manager server via SSH
        server_handle = ssh_server_manager.start_manager_server(manager_config)
        assert server_handle is not None, "Failed to create server handle"
        print(f"Server handle created: {server_handle.process_id}")

        # Wait for server to be ready
        print("\n=== Waiting for Server Ready ===")
        ready = ssh_server_manager.wait_for_server_ready(
            "TestManagerServerID", timeout=server_timeout
        )

        if not ready:
            startup_error = ssh_connection_manager.get_process_startup_error(
                server_handle
            )
            pytest.fail(
                f"Server failed to become ready. Startup error: {startup_error}"
            )

        assert ssh_connection_manager.is_process_alive(server_handle), (
            "Server process should be alive"
        )
        print("Server is ready and process is alive")

        # Create gRPC client connection
        print("\n=== Testing Initial gRPC Communication ===")
        channel = insecure_channel(f"localhost:{server_port}")
        stub = ManagerServiceStub(channel)

        # Send initial test request to verify server is working
        initial_request = DummyRequest(
            message="Pre-kill SSH test request",
            timestamp=int(time.time() * 1000),
        )

        initial_response = stub.MakeRequest(initial_request)
        assert "Manager server response" in initial_response.reply
        print(f"Initial request successful: {initial_response.reply}")

        # Send Kill request
        print("\n=== Sending Kill Request ===")
        kill_request = KillRequest(
            reason="Test SSH server kill functionality",
            grace_period_seconds=2,
        )

        # This should succeed and return a response
        kill_response = stub.Kill(kill_request)
        assert kill_response.shutdown_initiated, "Kill request should initiate shutdown"
        print(f"Kill request successful: {kill_response.message}")

        # Wait for server process to actually terminate
        print("\n=== Waiting for Server Process Termination ===")
        termination_timeout = 10.0
        start_time = time.time()

        while (
            time.time() - start_time
        ) < termination_timeout and ssh_connection_manager.is_process_alive(
            server_handle
        ):
            time.sleep(0.5)
            print(".", end="", flush=True)

        print()  # New line after dots

        if ssh_connection_manager.is_process_alive(server_handle):
            pytest.fail("SSH server process did not terminate within expected time")

        elapsed = time.time() - start_time
        print(f"SSH server process terminated after {elapsed:.1f}s")

        # Verify server manager also reports it as stopped
        if ssh_server_manager.is_server_running("TestManagerServerID"):
            pytest.fail("Server manager still reports server as running")

        print("Both process handle and server manager report server as stopped")

        # Now attempt second Kill request - this should fail with gRPC exception
        print("\n=== Testing Second Kill Request (Should Fail) ===")
        second_kill_request = KillRequest(
            reason="Second SSH kill attempt - should fail",
            grace_period_seconds=1,
        )

        exception_raised = False
        try:
            second_response = stub.Kill(second_kill_request)
            # If we get here, the server responded when it shouldn't have
            print(f"UNEXPECTED: Second kill got response: {second_response.message}")
            pytest.fail(
                "Second Kill request should have failed with gRPC exception, "
                f"but got response: {second_response.message}"
            )
        except RpcError as e:
            exception_raised = True
            print(f"Second Kill request properly failed with: {e.code()}")

            # Verify it's the right type of error (connection failure)
            assert e.code() in [
                StatusCode.UNAVAILABLE,
                StatusCode.CANCELLED,
                StatusCode.DEADLINE_EXCEEDED,
            ], f"Expected connection error, got: {e.code()}"

        if not exception_raised:
            pytest.fail(
                "Expected gRPC exception on second Kill request, but none was raised"
            )

        print("Test passed: Second request failed as expected")

    except Exception as e:
        pytest.fail(f"Test failed with unexpected error: {e}")

    finally:
        print("\n=== Cleanup ===")

        # Close gRPC channel
        if channel:
            try:
                channel.close()
                print("gRPC channel closed")
            except Exception as e:
                print(f"Warning: Error closing channel: {e}")

        # Clean up server manager
        try:
            ssh_server_manager.cleanup()
            print("SSH server manager cleanup completed")
        except Exception as e:
            print(f"Warning: Error during SSH server cleanup: {e}")

        # Clean up log manager
        try:
            log_manager.cleanup()
            print("Log manager cleanup completed")
        except Exception as e:
            print(f"Warning: Error during log cleanup: {e}")

    print("\nTest kill_ssh_server_via_grpc completed successfully")
