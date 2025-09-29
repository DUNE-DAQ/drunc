"""
Test for Manager Boot and Kill functionality.

This test verifies that:
1. Manager server can boot remote RootController servers via SSH using Boot RPC
2. Booted servers become operational and respond to requests
3. Manager Kill RPC terminates all booted servers before shutting down
4. All processes are properly cleaned up
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

# Import gRPC generated code
from drunc.tests.grpc.test_pb2 import BootRequest, DummyRequest, KillRequest
from drunc.tests.grpc.test_pb2_grpc import ManagerServiceStub, RootControllerServiceStub


def test_manager_boot_and_kill_via_grpc():
    """
    Test that verifies Manager can boot servers via Boot RPC and kill them via Kill RPC.

    This test:
    1. Starts a Manager server using multiprocessing
    2. Sends Boot request to start RootController via SSH
    3. Verifies RootController is operational
    4. Sends Kill request to Manager
    5. Verifies Manager terminates booted RootController first
    6. Verifies Manager itself terminates
    7. Checks all processes are cleaned up
    """

    # Environment configuration for SSH boot
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
    manager_port = 50090
    root_controller_port = 50091
    max_workers = 2
    server_timeout = 30.0

    print("=== Test Manager Boot and Kill via gRPC ===")
    print(f"Manager port: {manager_port}")
    print(f"RootController port: {root_controller_port}")
    print(f"Environment script: {env_setup_script}")

    # Create log file manager
    log_manager = LogFileManager()
    manager_log = log_manager.create_log_file("ManagerServer")
    root_log = log_manager.create_log_file("RootControllerServer")
    print(f"Manager log: {manager_log}")
    print(f"RootController log (local): {root_log}")

    # Create connection and server managers for Manager (multiprocessing)
    connection_manager = MultiprocessingConnectionManager(
        env_vars={"GRPC_TRACE": "http"}
    )
    server_manager = GrpcServerManager(connection_manager)

    # Configure Manager server
    manager_config = GrpcServerConfig(
        server_id="TestManagerServer",
        server_type=ServerType.MANAGER,
        host="localhost",
        port=manager_port,
        max_workers=max_workers,
        log_file=manager_log,
        server_options=[],
        client_options=[],
    )

    manager_handle = None
    manager_channel = None
    root_channel = None

    try:
        print("\n=== Starting Manager Server ===")

        # Start the Manager server
        manager_handle = server_manager.start_manager_server(manager_config)
        assert manager_handle is not None, "Failed to create Manager server handle"
        print(f"Manager server handle created: {manager_handle.process_id}")

        # Wait for Manager to be ready
        print("\n=== Waiting for Manager Ready ===")
        ready = server_manager.wait_for_server_ready(
            "TestManagerServer", timeout=server_timeout
        )

        assert ready, "Manager server failed to become ready within timeout"
        assert connection_manager.is_process_alive(manager_handle), (
            "Manager server process should be alive"
        )
        print("Manager server is ready and process is alive")

        # Create gRPC client connection to Manager
        print("\n=== Testing Manager Communication ===")

        manager_channel = insecure_channel(f"localhost:{manager_port}")
        manager_stub = ManagerServiceStub(manager_channel)

        # Send test request to verify Manager is working
        test_request = DummyRequest(
            message="Pre-boot test request",
            timestamp=int(time.time() * 1000),
        )

        test_response = manager_stub.MakeRequest(test_request)
        assert "Manager server response" in test_response.reply
        print(f"Manager communication successful: {test_response.reply}")

        # Send Boot request to start RootController via SSH
        print("\n=== Sending Boot Request for RootController ===")
        boot_request = BootRequest(
            process_id="BootedRootController",
            server_type="ROOT_CONTROLLER",
            port=root_controller_port,
            max_workers=max_workers,
            log_file=root_log,
            env_setup_script=env_setup_script,
            host="localhost",
            user=os.getenv("USER"),
        )

        # Add manager_port as extra parameter for RootController
        boot_request.extra_params["manager_port"] = str(manager_port)

        boot_response = manager_stub.Boot(boot_request)

        print(f"Boot response: success={boot_response.success}")
        print(f"Boot message: {boot_response.message}")
        print(f"Boot port: {boot_response.port}")
        print(f"RootController log (SSH remote): {root_log}")

        assert boot_response.success, f"Boot request failed: {boot_response.message}"
        assert boot_response.port == root_controller_port, "Boot response port mismatch"

        # Wait a moment for RootController to fully start
        time.sleep(2.0)

        # Verify RootController is operational by connecting to it
        print("\n=== Verifying RootController is Operational ===")
        root_channel = insecure_channel(f"localhost:{root_controller_port}")
        root_stub = RootControllerServiceStub(root_channel)

        root_test_request = DummyRequest(
            message="Test request to booted RootController",
            timestamp=int(time.time() * 1000),
        )

        root_response = root_stub.MakeRequest(root_test_request)
        assert "RootController server response" in root_response.reply
        print(f"RootController operational: {root_response.reply}")

        # Send Kill request to Manager
        print("\n=== Sending Kill Request to Manager ===")
        kill_request = KillRequest(
            reason="Test Manager boot and kill functionality",
            grace_period_seconds=3,
        )

        kill_response = manager_stub.Kill(kill_request)
        print(f"Kill response: shutdown_initiated={kill_response.shutdown_initiated}")
        print(f"Kill message: {kill_response.message}")

        assert kill_response.shutdown_initiated, (
            f"Kill request should initiate shutdown: {kill_response.message}"
        )

        # Verify RootController is no longer responding (killed by Manager)
        print("\n=== Verifying RootController Terminated ===")

        # Wait a moment for RootController to process the kill
        time.sleep(2.0)

        # Keep trying for up to 5 seconds to confirm RootController is down
        root_down = False
        for attempt in range(10):
            try:
                root_stub.MakeRequest(root_test_request, timeout=1.0)
                print(
                    f"Attempt {attempt + 1}: RootController still responding, waiting..."
                )
                time.sleep(0.5)
            except RpcError as e:
                # Expected - RootController should be terminated
                print(f"RootController properly terminated: {e.code()}")
                assert e.code() in [
                    StatusCode.UNAVAILABLE,
                    StatusCode.CANCELLED,
                    StatusCode.DEADLINE_EXCEEDED,
                ]
                root_down = True
                break

        if not root_down:
            pytest.fail("RootController should not be responding after Manager kill")

        # Wait for Manager process to terminate
        print("\n=== Waiting for Manager Termination ===")
        termination_timeout = 15.0
        start_time = time.time()

        while (
            time.time() - start_time
        ) < termination_timeout and connection_manager.is_process_alive(manager_handle):
            time.sleep(0.5)
            print(".", end="", flush=True)

        print()  # New line after dots

        if connection_manager.is_process_alive(manager_handle):
            pytest.fail("Manager process did not terminate within expected time")

        elapsed = time.time() - start_time
        print(f"Manager process terminated after {elapsed:.1f}s")

        # Verify Manager is no longer responding
        print("\n=== Verifying Manager Terminated ===")
        with pytest.raises(RpcError):
            manager_stub.MakeRequest(test_request, timeout=2.0)

        print("\n✓ Test passed: Manager successfully booted and killed RootController")

    except Exception as e:
        pytest.fail(f"Test failed with unexpected error: {e}")

    finally:
        print("\n=== Cleanup ===")

        # Close gRPC channels
        if root_channel:
            try:
                root_channel.close()
                print("RootController channel closed")
            except Exception as e:
                print(f"Warning: Error closing root channel: {e}")

        if manager_channel:
            try:
                manager_channel.close()
                print("Manager channel closed")
            except Exception as e:
                print(f"Warning: Error closing manager channel: {e}")

        # Clean up server manager
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

        # Clean up log manager
        try:
            log_manager.cleanup()
            print("Log manager cleanup completed")
        except Exception as e:
            print(f"Warning: Error during log cleanup: {e}")

    print("\nTest boot_and_kill_via_grpc completed successfully")


if __name__ == "__main__":
    """
    Run the test directly for debugging purposes.
    """
    try:
        test_manager_boot_and_kill_via_grpc()
        print("\n✓ Manager boot and kill test passed!")
    except Exception as e:
        print(f"\n✗ Manager boot and kill test failed: {e}")
        raise
