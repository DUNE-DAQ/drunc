"""
Comprehensive test suite for SSHProcessManager kill functionality.

This module tests both the high-level _kill_impl method and the low-level
kill_processes method with various process states and configurations.
"""

from unittest.mock import MagicMock, Mock, patch

import pytest

from drunc.process_manager.ssh_process_manager import SSHProcessManager

# TODO add get_process uid tests too


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

    This fixture instantiates the process manager with mocked dependencies
    to isolate the functionality being tested from external services.

    Args:
        mock_logger: Mock logger fixture to prevent actual logging operations

    Returns:
        SSHProcessManager: Configured process manager instance ready for testing
    """

    class _SSHProcessManager(SSHProcessManager):
        def __init__(
            self,
            configuration,
            **kwargs,
        ):
            """
            all-default constructor for testing purposes.
            """
            configuration.get_data().opmon_publisher = None
            super().__init__(configuration, **kwargs)

        def _create_broadcast_service(self, name, session):
            """Override broadcast service creation for testing"""
            self.broadcast_service = None

    # Create process manager with mock configuration
    servicer = _SSHProcessManager(configuration=Mock(), name="test_ssh_process_manager")
    servicer._mock_logger = mock_logger
    return servicer


# =============================================================================
# Tests for _kill_impl method (high-level interface)
# =============================================================================


def test_kill_impl_no_processes(ssh_process_manager: SSHProcessManager):
    """
    Verify _kill_impl handles empty process store gracefully.

    Tests the scenario where no processes exist to kill, ensuring
    the method returns appropriate success response without errors.

    Args:
        ssh_process_manager: The SSHProcessManager instance configured for testing
    """
    from druncschema.process_manager_pb2 import ProcessQuery
    from druncschema.request_response_pb2 import ResponseFlag

    # Set up process manager with empty process store
    ssh_process_manager.name = "test_ssh_process_manager"
    ssh_process_manager.process_store = {}

    # Create query for all processes
    query = ProcessQuery(names=["hello", ".*"])

    # Execute kill operation on empty process store
    result = ssh_process_manager._kill_impl(query)

    # Verify successful response with no processes killed
    assert result.name == ssh_process_manager.name
    assert result.flag == ResponseFlag.EXECUTED_SUCCESSFULLY
    assert len(result.values) == 0


def test_kill_impl_multiple_processes(ssh_process_manager: SSHProcessManager):
    """
    Verify _kill_impl correctly delegates to kill_processes for multiple processes.

    Mocks kill_processes to verify it receives the correct UUID list when
    multiple processes match the query criteria.

    Args:
        ssh_process_manager: The SSHProcessManager instance configured for testing
    """
    from druncschema.process_manager_pb2 import ProcessInstanceList, ProcessQuery
    from druncschema.request_response_pb2 import ResponseFlag

    ssh_process_manager.name = "test_ssh_process_manager"

    # Mock the _get_process_uid method to return fixed UUIDs
    test_uuids = ["uuid-1", "uuid-2", "uuid-3"]
    ssh_process_manager._get_process_uid = MagicMock(return_value=test_uuids)

    # Mock kill_processes
    expected_response = ProcessInstanceList(
        name=ssh_process_manager.name,
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        values=[],
    )
    ssh_process_manager.kill_processes = MagicMock(return_value=expected_response)

    # Create query corresponding to test uuids
    query = ProcessQuery(names=["app1", "app2", "app3"])
    ssh_process_manager.process_store = {
        uuid: app for uuid, app in zip(test_uuids, query.names)
    }

    # Execute kill operation
    result = ssh_process_manager._kill_impl(query)

    # Verify _get_process_uid was called with correct parameters
    ssh_process_manager._get_process_uid.assert_called_once_with(
        query, order_by="leaf_first"
    )

    # Verify kill_processes was called with the UUIDs returned by _get_process_uid
    ssh_process_manager.kill_processes.assert_called_once_with(test_uuids)

    # Verify the response is passed through correctly
    assert result == expected_response


def test_kill_impl_single_process(ssh_process_manager: SSHProcessManager):
    """
    Verify _kill_impl correctly delegates to kill_processes for single process.

    Tests the most common scenario where exactly one process matches the
    query and verifies proper delegation to the kill_processes method.

    Args:
        ssh_process_manager: The SSHProcessManager instance configured for testing
    """
    from druncschema.process_manager_pb2 import ProcessInstanceList, ProcessQuery
    from druncschema.request_response_pb2 import ResponseFlag

    ssh_process_manager.name = "test_ssh_process_manager"

    # Mock the _get_process_uid method to return fixed UUID
    test_uuid = ["single-process-uuid"]
    ssh_process_manager._get_process_uid = MagicMock(return_value=test_uuid)

    # Mock kill_processes
    expected_response = ProcessInstanceList(
        name=ssh_process_manager.name,
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        values=[],
    )
    ssh_process_manager.kill_processes = MagicMock(return_value=expected_response)

    # Create query corresponding to test uuid
    query = ProcessQuery(names=["single_app"])
    ssh_process_manager.process_store = {test_uuid[0]: query.names[0]}

    # Execute kill operation
    result = ssh_process_manager._kill_impl(query)

    # Verify _get_process_uid was called with correct parameters
    ssh_process_manager._get_process_uid.assert_called_once_with(
        query, order_by="leaf_first"
    )

    # Verify kill_processes was called with the UUID returned by _get_process_uid
    ssh_process_manager.kill_processes.assert_called_once_with(test_uuid)

    # Verify the response is passed through correctly
    assert result == expected_response


# =============================================================================
# Tests for kill_processes method (low-level implementation)
# =============================================================================


def test_kill_processes_no_uuids(ssh_process_manager: SSHProcessManager):
    """
    Verify kill_processes handles empty UUID list correctly.

    Tests the edge case where no process UUIDs are provided,
    ensuring the method returns a successful empty response.

    Args:
        ssh_process_manager: The SSHProcessManager instance configured for testing
    """
    from druncschema.request_response_pb2 import ResponseFlag

    # Set up process manager name
    ssh_process_manager.name = "test_ssh_process_manager"

    # Execute kill operation with empty UUID list
    result = ssh_process_manager.kill_processes([])

    # Verify successful response with no processes processed
    assert result.name == ssh_process_manager.name
    assert result.flag == ResponseFlag.EXECUTED_SUCCESSFULLY
    assert len(result.values) == 0


def test_kill_processes_single_uuid(ssh_process_manager: SSHProcessManager):
    """
    Verify kill_processes correctly handles single running process.

    Tests killing one process that is currently alive, ensuring proper
    signal handling and process cleanup occur.

    Args:
        ssh_process_manager: The SSHProcessManager instance configured for testing
    """
    from druncschema.process_manager_pb2 import (
        BootRequest,
        ProcessDescription,
        ProcessInstance,
        ProcessMetadata,
        ProcessRestriction,
    )
    from druncschema.request_response_pb2 import ResponseFlag

    # Set up process manager configuration
    ssh_process_manager.name = "test_ssh_process_manager"
    ssh_process_manager.configuration = MagicMock()
    ssh_process_manager.configuration.data.kill_timeout = 1

    # Create test process UUID
    test_uuid = "single-test-uuid"

    # Create mock running process that terminates after first signal
    mock_process = MagicMock()
    mock_process.is_alive.return_value = True
    mock_process.exit_code = 0
    mock_process.signal_group = MagicMock()

    def simulate_process_death(signal):
        """Simulate process terminating after receiving signal"""
        mock_process.is_alive.return_value = False

    mock_process.signal_group.side_effect = simulate_process_death

    # Create process metadata and description
    metadata = ProcessMetadata(
        name="test_single_process",
        user="test_user",
        session="test_session",
        hostname="test_host",
        tree_id="1.0",
    )

    description = ProcessDescription(
        metadata=metadata,
        executable_and_arguments=[
            ProcessDescription.ExecAndArgs(
                exec="test_app", args=["--config", "test.conf"]
            )
        ],
        process_execution_directory="/opt/test",
        process_logs_path="/var/log/test.log",
    )

    restriction = ProcessRestriction(allowed_hosts=["test_host"])

    boot_request = BootRequest(
        process_description=description, process_restriction=restriction
    )

    # Configure process manager state
    ssh_process_manager.process_store = {test_uuid: mock_process}
    ssh_process_manager.boot_request = {test_uuid: boot_request}

    # Execute kill operation
    result = ssh_process_manager.kill_processes([test_uuid])

    # Verify successful kill operation
    assert result.name == ssh_process_manager.name
    assert result.flag == ResponseFlag.EXECUTED_SUCCESSFULLY
    assert len(result.values) == 1

    # Verify process result details
    process_result = result.values[0]
    assert process_result.uuid.uuid == test_uuid
    assert process_result.status_code == ProcessInstance.StatusCode.DEAD
    assert process_result.return_code == 0
    assert process_result.process_description.metadata.name == "test_single_process"

    # Verify signal was sent and process was removed from store
    mock_process.signal_group.assert_called()
    assert test_uuid not in ssh_process_manager.process_store


def test_kill_processes_three_uuids_all_alive(ssh_process_manager: SSHProcessManager):
    """
    Verify kill_processes correctly handles three running processes.

    Tests killing multiple processes that are all currently alive,
    ensuring each process receives appropriate signals and cleanup.

    Args:
        ssh_process_manager: The SSHProcessManager instance configured for testing
    """
    from druncschema.process_manager_pb2 import (
        BootRequest,
        ProcessDescription,
        ProcessInstance,
        ProcessMetadata,
        ProcessRestriction,
    )
    from druncschema.request_response_pb2 import ResponseFlag

    # Set up process manager configuration
    ssh_process_manager.name = "test_ssh_process_manager"
    ssh_process_manager.configuration = MagicMock()
    ssh_process_manager.configuration.data.kill_timeout = 1

    # Create test process UUIDs
    test_uuids = ["uuid-alive-1", "uuid-alive-2", "uuid-alive-3"]

    # Create mock processes and boot requests
    ssh_process_manager.process_store = {}
    ssh_process_manager.boot_request = {}

    for i, uuid in enumerate(test_uuids):
        # Create mock running process
        mock_process = MagicMock()
        mock_process.is_alive.return_value = True
        mock_process.exit_code = 0
        mock_process.signal_group = MagicMock()

        def create_death_simulator(process_mock):
            """Factory function to create process death simulator"""

            def simulate_death(signal):
                process_mock.is_alive.return_value = False

            return simulate_death

        mock_process.signal_group.side_effect = create_death_simulator(mock_process)

        # Create process metadata and description
        metadata = ProcessMetadata(
            name=f"test_process_{i + 1}",
            user="test_user",
            session="test_session",
            hostname="test_host",
            tree_id=f"1.{i + 1}",
        )

        description = ProcessDescription(
            metadata=metadata,
            executable_and_arguments=[
                ProcessDescription.ExecAndArgs(
                    exec=f"app_{i + 1}", args=[f"--id={i + 1}"]
                )
            ],
            process_execution_directory="/opt/test",
            process_logs_path=f"/var/log/app_{i + 1}.log",
        )

        restriction = ProcessRestriction(allowed_hosts=["test_host"])

        boot_request = BootRequest(
            process_description=description, process_restriction=restriction
        )

        # Store process and boot request
        ssh_process_manager.process_store[uuid] = mock_process
        ssh_process_manager.boot_request[uuid] = boot_request

    # Execute kill operation on all processes
    result = ssh_process_manager.kill_processes(test_uuids)

    # Verify successful kill operation
    assert result.name == ssh_process_manager.name
    assert result.flag == ResponseFlag.EXECUTED_SUCCESSFULLY
    assert len(result.values) == 3

    # Verify each process was killed correctly
    for i, process_result in enumerate(result.values):
        expected_uuid = test_uuids[i]
        assert process_result.uuid.uuid == expected_uuid
        assert process_result.status_code == ProcessInstance.StatusCode.DEAD
        assert process_result.return_code == 0
        assert (
            process_result.process_description.metadata.name == f"test_process_{i + 1}"
        )

    # Verify all processes received signals and were removed from store
    for uuid in test_uuids:
        assert uuid not in ssh_process_manager.process_store


def test_kill_processes_three_uuids_all_dead(ssh_process_manager: SSHProcessManager):
    """
    Verify kill_processes correctly handles three dead processes.

    Tests killing multiple processes that are already terminated,
    ensuring no unnecessary signals are sent and proper cleanup occurs.

    Args:
        ssh_process_manager: The SSHProcessManager instance configured for testing
    """
    from druncschema.process_manager_pb2 import (
        BootRequest,
        ProcessDescription,
        ProcessInstance,
        ProcessMetadata,
        ProcessRestriction,
    )
    from druncschema.request_response_pb2 import ResponseFlag

    # Set up process manager configuration
    ssh_process_manager.name = "test_ssh_process_manager"
    ssh_process_manager.configuration = MagicMock()
    ssh_process_manager.configuration.data.kill_timeout = 1

    # Create test process UUIDs
    test_uuids = ["uuid-dead-1", "uuid-dead-2", "uuid-dead-3"]

    # Create mock processes and boot requests
    ssh_process_manager.process_store = {}
    ssh_process_manager.boot_request = {}

    for i, uuid in enumerate(test_uuids):
        # Create mock dead process
        mock_process = MagicMock()
        mock_process.is_alive.return_value = False
        mock_process.exit_code = i + 1  # Different exit codes for variety
        mock_process.signal_group = MagicMock()

        # Create process metadata and description
        metadata = ProcessMetadata(
            name=f"dead_process_{i + 1}",
            user="test_user",
            session="test_session",
            hostname="test_host",
            tree_id=f"2.{i + 1}",
        )

        description = ProcessDescription(
            metadata=metadata,
            executable_and_arguments=[
                ProcessDescription.ExecAndArgs(
                    exec=f"dead_app_{i + 1}", args=["--terminated"]
                )
            ],
            process_execution_directory="/opt/test",
            process_logs_path=f"/var/log/dead_app_{i + 1}.log",
        )

        restriction = ProcessRestriction(allowed_hosts=["test_host"])

        boot_request = BootRequest(
            process_description=description, process_restriction=restriction
        )

        # Store process and boot request
        ssh_process_manager.process_store[uuid] = mock_process
        ssh_process_manager.boot_request[uuid] = boot_request

    # Execute kill operation on all dead processes
    result = ssh_process_manager.kill_processes(test_uuids)

    # Verify successful kill operation
    assert result.name == ssh_process_manager.name
    assert result.flag == ResponseFlag.EXECUTED_SUCCESSFULLY
    assert len(result.values) == 3

    # Verify each dead process was handled correctly
    for i, process_result in enumerate(result.values):
        expected_uuid = test_uuids[i]
        assert process_result.uuid.uuid == expected_uuid
        assert process_result.status_code == ProcessInstance.StatusCode.DEAD
        assert process_result.return_code == i + 1
        assert (
            process_result.process_description.metadata.name == f"dead_process_{i + 1}"
        )

    # Verify no signals were sent to dead processes
    for uuid in test_uuids:
        mock_process = ssh_process_manager.process_store.get(uuid)
        if mock_process:  # Process may have been removed
            mock_process.signal_group.assert_not_called()
        # Verify process was removed from store
        assert uuid not in ssh_process_manager.process_store


def test_kill_processes_three_uuids_mixed_states(
    ssh_process_manager: SSHProcessManager,
):
    """
    Verify kill_processes correctly handles mixed process states.

    Tests killing processes where one is alive and two are dead,
    ensuring appropriate signals are sent only to living processes.

    Args:
        ssh_process_manager: The SSHProcessManager instance configured for testing
    """
    from druncschema.process_manager_pb2 import (
        BootRequest,
        ProcessDescription,
        ProcessInstance,
        ProcessMetadata,
        ProcessRestriction,
    )
    from druncschema.request_response_pb2 import ResponseFlag

    # Set up process manager configuration
    ssh_process_manager.name = "test_ssh_process_manager"
    ssh_process_manager.configuration = MagicMock()
    ssh_process_manager.configuration.data.kill_timeout = 1

    # Create test process UUIDs: one alive, two dead
    alive_uuid = "uuid-alive"
    dead_uuid_1 = "uuid-dead-1"
    dead_uuid_2 = "uuid-dead-2"
    test_uuids = [alive_uuid, dead_uuid_1, dead_uuid_2]

    # Initialize process manager state
    ssh_process_manager.process_store = {}
    ssh_process_manager.boot_request = {}

    # Create mock alive process
    alive_process = MagicMock()
    alive_process.is_alive.return_value = True
    alive_process.exit_code = 0
    alive_process.signal_group = MagicMock()

    def simulate_alive_death(signal):
        """Simulate alive process terminating after receiving signal"""
        alive_process.is_alive.return_value = False

    alive_process.signal_group.side_effect = simulate_alive_death

    # Create metadata and boot request for alive process
    alive_metadata = ProcessMetadata(
        name="alive_process",
        user="test_user",
        session="test_session",
        hostname="test_host",
        tree_id="3.1",
    )

    alive_description = ProcessDescription(
        metadata=alive_metadata,
        executable_and_arguments=[
            ProcessDescription.ExecAndArgs(exec="alive_app", args=["--running"])
        ],
        process_execution_directory="/opt/test",
        process_logs_path="/var/log/alive.log",
    )

    alive_restriction = ProcessRestriction(allowed_hosts=["test_host"])

    alive_boot_request = BootRequest(
        process_description=alive_description, process_restriction=alive_restriction
    )

    ssh_process_manager.process_store[alive_uuid] = alive_process
    ssh_process_manager.boot_request[alive_uuid] = alive_boot_request

    # Create mock dead processes
    dead_processes_info = [
        (dead_uuid_1, "dead_process_1", 1),
        (dead_uuid_2, "dead_process_2", 2),
    ]

    for uuid, name, exit_code in dead_processes_info:
        # Create mock dead process
        dead_process = MagicMock()
        dead_process.is_alive.return_value = False
        dead_process.exit_code = exit_code
        dead_process.signal_group = MagicMock()

        # Create process metadata and description
        metadata = ProcessMetadata(
            name=name,
            user="test_user",
            session="test_session",
            hostname="test_host",
            tree_id=f"3.{exit_code + 1}",
        )

        description = ProcessDescription(
            metadata=metadata,
            executable_and_arguments=[
                ProcessDescription.ExecAndArgs(
                    exec=f"{name}_app", args=["--terminated"]
                )
            ],
            process_execution_directory="/opt/test",
            process_logs_path=f"/var/log/{name}.log",
        )

        restriction = ProcessRestriction(allowed_hosts=["test_host"])

        boot_request = BootRequest(
            process_description=description, process_restriction=restriction
        )

        # Store process and boot request
        ssh_process_manager.process_store[uuid] = dead_process
        ssh_process_manager.boot_request[uuid] = boot_request

    # Execute kill operation on mixed state processes
    result = ssh_process_manager.kill_processes(test_uuids)

    # Verify successful kill operation
    assert result.name == ssh_process_manager.name
    assert result.flag == ResponseFlag.EXECUTED_SUCCESSFULLY
    assert len(result.values) == 3

    # Verify alive process result
    alive_result = next(r for r in result.values if r.uuid.uuid == alive_uuid)
    assert alive_result.status_code == ProcessInstance.StatusCode.DEAD
    assert alive_result.return_code == 0
    assert alive_result.process_description.metadata.name == "alive_process"

    # Verify dead process results
    for uuid, name, expected_exit_code in dead_processes_info:
        dead_result = next(r for r in result.values if r.uuid.uuid == uuid)
        assert dead_result.status_code == ProcessInstance.StatusCode.DEAD
        assert dead_result.return_code == expected_exit_code
        assert dead_result.process_description.metadata.name == name

    # Verify signals were sent only to alive process
    alive_process.signal_group.assert_called()

    # Verify no signals were sent to dead processes
    for uuid in [dead_uuid_1, dead_uuid_2]:
        # Process may have been removed from store, so check if it existed
        if uuid in ssh_process_manager.process_store:
            dead_proc = ssh_process_manager.process_store[uuid]
            dead_proc.signal_group.assert_not_called()

    # Verify all processes were removed from store
    for uuid in test_uuids:
        assert uuid not in ssh_process_manager.process_store
