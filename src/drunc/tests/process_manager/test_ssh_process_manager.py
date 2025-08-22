"""
This suite tests the specific implementations of each endpoint in
process manager with an ssh connection
"""

from unittest.mock import MagicMock, Mock, patch

import pytest

from drunc.process_manager.ssh_process_manager import SSHProcessManager


@pytest.fixture(scope="module")
def mock_logger():
    """
    Create a mock logger that intercepts get_logger calls during testing.

    This fixture patches the logger creation to prevent actual logging operations
    during tests while still allowing the code under test to interact with a
    logger interface.

    Yields:
        MagicMock: Patched get_logger function with accessible logger instance
    """
    with patch("drunc.process_manager.process_manager.get_logger") as mock_get_logger:
        # Create a mock logger instance that behaves like a real logger
        mock_logger_instance = MagicMock()
        mock_get_logger.return_value = mock_logger_instance
        mock_get_logger.logger_instance = mock_logger_instance
        yield mock_get_logger


@pytest.fixture(scope="function")
def ssh_process_manager(mock_logger) -> SSHProcessManager:
    """
    Create and configure a SSHProcessManager instance for testing.

    This fixture instantiates the process manager servicer with a mocked logger
    The servicer implements the ProcessManager gRPC service interface.

    Args:
        mock_logger: Mock logger fixture to prevent actual logging operations

    Returns:
        ConcreteProcessManager: Configured servicer instance ready for testing
    """

    # mock out the broadcast service creation
    class _SSHProcessManager(SSHProcessManager):
        def _create_broadcast_service(self, name, session):
            self.broadcast_service = None

    servicer = _SSHProcessManager(configuration=Mock(), name="test_ssh_process_manager")
    servicer._mock_logger = mock_logger
    return servicer


def test_kill_impl(ssh_process_manager: SSHProcessManager):
    """
    Test the kill implementation for SSH process manager.

    Args:
        ssh_process_manager: The ProcessManager servicer instance to use for testing
    """

    from druncschema.process_manager_pb2 import (
        BootRequest,
        ProcessDescription,
        ProcessInstance,
        ProcessMetadata,
        ProcessRestriction,
    )
    from druncschema.request_response_pb2 import ResponseFlag

    # Create SSH process manager instance for testing the actual implementation
    ssh_process_manager.name = "test_ssh_process_manager"

    # Test scenario 1: Kill a running process successfully
    running_uuid = "running-process-uuid-123"

    # Create mock process that simulates a running process
    mock_running_process = MagicMock()
    mock_running_process.is_alive.return_value = True
    mock_running_process.signal_group = MagicMock()

    # Simulate process responding to SIGQUIT (first signal in sequence)
    def mock_signal_response(signal):
        """Simulate process dying after receiving SIGQUIT signal"""
        if hasattr(mock_signal_response, "call_count"):
            mock_signal_response.call_count += 1
        else:
            mock_signal_response.call_count = 1
        # After first signal, process should be dead
        mock_running_process.is_alive.return_value = False
        mock_running_process.exit_code = 0

    mock_running_process.signal_group.side_effect = mock_signal_response

    # Create mock process metadata for the running process
    running_metadata = ProcessMetadata(
        name="test_running_process",
        user="test_user",
        session="test_session",
        hostname="test_host",
        tree_id="1.0",
    )

    # Create mock process description for the running process
    running_description = ProcessDescription(
        metadata=running_metadata,
        executable_and_arguments=[
            ProcessDescription.ExecAndArgs(
                exec="test_app", args=["--config", "prod.conf"]
            )
        ],
        process_execution_directory="/opt/test_app",
        process_logs_path="/var/log/test_app.log",
        env={"ENV": "production", "LOG_LEVEL": "INFO"},
    )

    # Create mock process restriction
    running_restriction = ProcessRestriction(
        allowed_hosts=["test_host"], allowed_host_types=["production"]
    )

    # Create mock boot request for the running process
    running_boot_request = BootRequest(
        process_description=running_description, process_restriction=running_restriction
    )

    # Test scenario 2: Try to kill an already dead process
    dead_uuid = "dead-process-uuid-456"

    # Create mock process that simulates a dead process
    mock_dead_process = MagicMock()
    mock_dead_process.is_alive.return_value = False
    mock_dead_process.exit_code = 1  # Simulate process that exited with error

    # Create metadata for dead process
    dead_metadata = ProcessMetadata(
        name="test_dead_process",
        user="test_user",
        session="test_session",
        hostname="test_host",
        tree_id="1.1",
    )

    # Create description for dead process
    dead_description = ProcessDescription(
        metadata=dead_metadata,
        executable_and_arguments=[
            ProcessDescription.ExecAndArgs(exec="test_service", args=["--daemon"])
        ],
        process_execution_directory="/opt/test_service",
        process_logs_path="/var/log/test_service.log",
    )

    # Create restriction for dead process
    dead_restriction = ProcessRestriction(
        allowed_hosts=["test_host"], allowed_host_types=["production"]
    )

    # Create boot request for dead process
    dead_boot_request = BootRequest(
        process_description=dead_description, process_restriction=dead_restriction
    )

    # Set up the SSH process manager's internal state
    ssh_process_manager.process_store = {
        running_uuid: mock_running_process,
        dead_uuid: mock_dead_process,
    }

    ssh_process_manager.boot_request = {
        running_uuid: running_boot_request,
        dead_uuid: dead_boot_request,
    }

    # Mock configuration for kill timeout
    ssh_process_manager.configuration = MagicMock()
    ssh_process_manager.configuration.data.kill_timeout = (
        2  # 2 second timeout between signals
    )

    # Execute the kill_processes method with both UUIDs
    test_uuids = [running_uuid, dead_uuid]
    result = ssh_process_manager.kill_processes(test_uuids)

    # Verify the result structure
    assert result.name == ssh_process_manager.name
    assert result.flag == ResponseFlag.EXECUTED_SUCCESSFULLY
    assert len(result.values) == 2

    # Find results for each process by UUID
    running_result = None
    dead_result = None

    for process_instance in result.values:
        if process_instance.uuid.uuid == running_uuid:
            running_result = process_instance
        elif process_instance.uuid.uuid == dead_uuid:
            dead_result = process_instance

    # Verify running process was killed correctly
    assert running_result is not None, "Should have result for running process"
    assert running_result.status_code == ProcessInstance.StatusCode.DEAD
    assert running_result.return_code == 0
    assert running_result.process_description.metadata.name == "test_running_process"

    # Verify dead process was handled correctly
    assert dead_result is not None, "Should have result for dead process"
    assert dead_result.status_code == ProcessInstance.StatusCode.DEAD
    assert dead_result.return_code == 1
    assert dead_result.process_description.metadata.name == "test_dead_process"

    # Verify that signal_group was called on the running process
    mock_running_process.signal_group.assert_called()

    # Verify that signal_group was NOT called on the dead process
    mock_dead_process.signal_group.assert_not_called()

    # Verify that processes were removed from process_store after killing
    assert running_uuid not in ssh_process_manager.process_store
    assert dead_uuid not in ssh_process_manager.process_store
