import os
import time
import uuid
from pathlib import Path

import pytest
from grpc import RpcError, StatusCode, insecure_channel

from drunc.grpc_testing_tools.available_grpc_servers import ServerType
from drunc.grpc_testing_tools.grpc_log_file_manager import LogFileManager
from drunc.grpc_testing_tools.grpc_server_manager import (
    GrpcServerConfig,
    GrpcServerManager,
)
from drunc.grpc_testing_tools.multiprocessing_connection_manager import (
    MultiprocessingConnectionManager,
)
from drunc.grpc_testing_tools.test_services_pb2 import (
    BootRequest,
    DummyRequest,
    KillRequest,
    ProcessDescription,
    ProcessMetadata,
    ProcessRestriction,
    ProcessUUID,
    Token,
)

# Import gRPC generated code
from drunc.grpc_testing_tools.test_services_pb2_grpc import (
    ManagerServiceStub,
    RootControllerServiceStub,
)


def execute_manager_boot_and_kill_test(
    grpc_process_manager_service_resources, lifetime_manager_type: str
):
    """
    Execute and verify Manager boot and kill functionality via gRPC.

    Tests complete lifecycle:
    1) Manager server startup
    2) SSH-based RootController server boot via Boot RPC
    3) Verification of operational state
    4) Graceful shutdown via Kill RPC
    5) Resource cleanup.

    Args:
        grpc_process_manager_service_resources: Fixture providing process manager serving via gRPC
        resources that need to be started/ cleaned up.
        lifetime_manager_type: Type of SSH manager to use ("paramiko" or "shell")
    """
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    ENV_SCRIPT_DIR = PROJECT_ROOT.parent.parent

    # Environment configuration for SSH boot
    env_script_dir = ENV_SCRIPT_DIR
    env_file = "env.sh"
    env_setup_script = f"cd {env_script_dir} && source {env_file}"

    # Verify environment setup exists
    assert os.path.exists(env_script_dir), (
        f"Environment script directory not found: {env_script_dir}. Update path as needed."
    )
    assert os.path.exists(os.path.join(env_script_dir, env_file)), (
        f"Environment script file not found: {env_file}. Update path as needed."
    )

    ROOT_CONTROLLER_SCRIPT = (
        f"{PROJECT_ROOT}/src/drunc/grpc_testing_tools/root_controller_server_cli.py"
    )

    assert os.path.exists(ROOT_CONTROLLER_SCRIPT), (
        f"RootController server script not found: {ROOT_CONTROLLER_SCRIPT}. Update path as needed."
    )

    # Test configuration
    manager_port = 50090
    root_controller_port = 50091
    max_workers = 2
    server_timeout = 60.0

    # Register ports for cleanup
    grpc_process_manager_service_resources.ports_to_cleanup = [
        manager_port,
        root_controller_port,
    ]

    print(f"\n=== Test Manager Boot and Kill via gRPC ({lifetime_manager_type}) ===")
    print(f"Manager port: {manager_port}")
    print(f"RootController port: {root_controller_port}")
    print(f"Lifetime manager type: {lifetime_manager_type}")
    print(f"Environment script: {env_setup_script}")

    # Create log file manager
    grpc_process_manager_service_resources.log_manager = LogFileManager()
    manager_log = grpc_process_manager_service_resources.log_manager.create_log_file(
        "ManagerServer"
    )
    root_log = grpc_process_manager_service_resources.log_manager.create_log_file(
        "RootControllerServer"
    )
    print(f"Manager log: {manager_log}")
    print(f"RootController log (local): {root_log}")

    # Create connection and server managers for Manager (multiprocessing)
    grpc_process_manager_service_resources.connection_manager = (
        MultiprocessingConnectionManager(env_vars={"GRPC_TRACE": "http"})
    )
    grpc_process_manager_service_resources.server_manager = GrpcServerManager(
        grpc_process_manager_service_resources.connection_manager
    )

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

    print("\n=== Starting Manager Server ===")

    # Start the Manager server with specified lifetime manager type
    manager_handle = (
        grpc_process_manager_service_resources.server_manager.start_manager_server(
            manager_config, lifetime_manager_type=lifetime_manager_type
        )
    )
    assert manager_handle is not None, "Failed to create Manager server handle"
    print(f"Manager server handle created: {manager_handle.process_id}")

    # Wait for Manager to be ready
    print("\n=== Waiting for Manager Ready ===")
    ready = grpc_process_manager_service_resources.server_manager.wait_for_server_ready(
        "TestManagerServer", timeout=server_timeout
    )

    assert ready, "Manager server failed to become ready within timeout"
    assert grpc_process_manager_service_resources.connection_manager.is_process_alive(
        manager_handle
    ), "Manager server process should be alive"
    print("Manager server is ready and process is alive")

    # Create gRPC client connection to Manager
    print("\n=== Testing Manager Communication ===")

    grpc_process_manager_service_resources.manager_channel = insecure_channel(
        f"localhost:{manager_port}"
    )
    manager_stub = ManagerServiceStub(
        grpc_process_manager_service_resources.manager_channel
    )

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

    # Create process UUID
    process_uuid = ProcessUUID(uuid=str(uuid.uuid4()))

    # Create process metadata
    process_metadata = ProcessMetadata(
        uuid=process_uuid,
        user=os.getenv("USER"),
        session="test_session",
        name="RootController",
        hostname="localhost",
    )

    # Create process description with working directory and command
    process_description = ProcessDescription(
        metadata=process_metadata,
        env={
            "GRPC_TRACE": "http",
            "PYTHONPATH": str(ENV_SCRIPT_DIR),
        },
        executable_and_arguments=[
            ProcessDescription.ExecAndArgs(
                args=[
                    " && ".join(
                        [
                            f"cd {env_script_dir}",
                            f"source {env_file}",
                            " ".join(
                                [
                                    f"python3 {ROOT_CONTROLLER_SCRIPT}",
                                    f"--port {root_controller_port}",
                                    f"--workers {max_workers}",
                                    f"--log-file {root_log}",
                                    f"--manager-port {manager_port}",
                                ]
                            ),
                        ]
                    ),
                ],
            )
        ],
        process_execution_directory="/",
        process_logs_path=root_log,
    )

    # Create process restriction (empty for this test)
    process_restriction = ProcessRestriction()

    # Create token for request authentication
    token = Token(token="test_token_123")

    # Create the boot request
    boot_request = BootRequest(
        token=token,
        process_description=process_description,
        process_restriction=process_restriction,
    )

    # Send the boot request
    boot_response = manager_stub.boot(boot_request)

    print(f"Boot response: success={boot_response.flag.success}")
    print(f"Boot message: {boot_response.flag.message}")
    print(f"Process instances: {len(boot_response.values)}")
    if boot_response.values:
        print(f"Boot process UUID: {boot_response.values[0].uuid.uuid}")
    print(f"RootController log (SSH remote): {root_log}")

    # Check for fork() issue after triggering SSH boot on gRPC server
    error = grpc_process_manager_service_resources.log_manager.check_for_errors()
    if error is not None:
        pytest.fail(f"Error detected after Boot request. Error: {error}")

    assert boot_response.flag.success, (
        f"Boot request failed: {boot_response.flag.message}"
    )

    # Verify RootController is operational by connecting to it
    print("\n=== Verifying RootController is Operational ===")
    grpc_process_manager_service_resources.root_channel = insecure_channel(
        f"localhost:{root_controller_port}"
    )
    root_stub = RootControllerServiceStub(
        grpc_process_manager_service_resources.root_channel
    )

    root_test_request = DummyRequest(message="Test request to booted RootController")

    # Give RootController a few tries to become responsive
    max_root_controller_retries = 10
    for attempt in range(max_root_controller_retries):
        try:
            root_response = root_stub.MakeRequest(root_test_request, timeout=5.0)
            break  # Success
        except RpcError as e:
            print(f"Attempt {attempt + 1} to contact RootController failed: {e.code()}")
            if attempt == max_root_controller_retries - 1:
                pytest.fail("RootController not responding after multiple attempts")
            time.sleep(5.0)

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

    # Keep trying for up to 5 seconds to confirm RootController is down
    root_down = False
    for attempt in range(10):
        try:
            root_stub.MakeRequest(root_test_request, timeout=1.0)
            print(f"Attempt {attempt + 1}: RootController still responding, waiting...")
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
        (time.time() - start_time) < termination_timeout
        and grpc_process_manager_service_resources.connection_manager.is_process_alive(
            manager_handle
        )
    ):
        time.sleep(0.5)
        print(".", end="", flush=True)

    print()  # New line after dots

    if grpc_process_manager_service_resources.connection_manager.is_process_alive(
        manager_handle
    ):
        pytest.fail("Manager process did not terminate within expected time")

    elapsed = time.time() - start_time
    print(f"Manager process terminated after {elapsed:.1f}s")

    # Verify Manager is no longer responding
    print("\n=== Verifying Manager Terminated ===")
    with pytest.raises(RpcError):
        manager_stub.MakeRequest(test_request, timeout=2.0)

    print(
        f"\n✓ Test passed: Manager ({lifetime_manager_type}) successfully booted and killed RootController"
    )


@pytest.mark.paramiko
def test_manager_boot_and_kill_via_grpc_paramiko(
    grpc_process_manager_service_resources,
):
    """
    Test Manager boot and kill functionality using Paramiko SSH implementation.

    Verifies Manager can boot servers via Boot RPC using Paramiko SSH backend
    and terminate them via Kill RPC with proper cleanup.

    Args:
        grpc_process_manager_service_resources: Fixture providing managed gRPC test resources
    """
    execute_manager_boot_and_kill_test(
        grpc_process_manager_service_resources, "paramiko"
    )


def test_manager_boot_and_kill_via_grpc_shell(grpc_process_manager_service_resources):
    """
    Test Manager boot and kill functionality using shell SSH implementation.

    Verifies Manager can boot servers via Boot RPC using shell SSH backend
    and terminate them via Kill RPC with proper cleanup.

    Args:
        grpc_process_manager_service_resources: Fixture providing managed gRPC test resources
    """
    execute_manager_boot_and_kill_test(grpc_process_manager_service_resources, "shell")
