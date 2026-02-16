import getpass
import tempfile
import uuid
from pathlib import Path

import pytest
from druncschema.process_manager_pb2 import (
    BootRequest,
    ProcessDescription,
    ProcessMetadata,
)

from drunc.grpc_testing_tools.connection_utils import wait_for


def create_boot_request(process_name, tree_id, log_file, test_file_path):
    """
    Create a boot request for a test process with specified parameters.

    Args:
        process_name: Name identifier for the process
        tree_id: Hierarchical process identifier (role computed from this)
        log_file: Path to log file on remote host
        test_file_path: Path to test file directory (for locating simple_process.py)

    Returns:
        Configured BootRequest ready for process execution
    """
    simple_process_script = test_file_path.parent / "simple_process.py"

    boot_request = BootRequest(
        process_description=ProcessDescription(
            metadata=ProcessMetadata(
                name=process_name,
                session="test_session",
                user=getpass.getuser(),
                hostname="localhost",
                tree_id=tree_id,
            ),
            process_logs_path=log_file,
        )
    )

    boot_request.process_description.executable_and_arguments.add(
        exec=f"python3 {simple_process_script}", args=[process_name]
    )

    return boot_request


def verify_log_output(ssh_manager, process_uuids, process_info, timeout=10.0):
    """
    Verify that all processes have generated expected log output.

    Polls remote log files until they contain startup and heartbeat messages,
    or until timeout is reached.

    Args:
        ssh_manager: SSH process lifetime manager instance
        process_uuids: List of process UUIDs to verify
        process_info: Dictionary mapping UUIDs to process information including log_file
        timeout: Maximum time in seconds to wait for log verification

    Raises:
        AssertionError: If log verification fails for any process
    """

    def check_log_lines(log_file):
        """Check if log file contains expected startup and heartbeat messages."""
        log_lines = ssh_manager.read_log_file(
            hostname="localhost",
            user=getpass.getuser(),
            log_file=log_file,
            num_lines=100,
        )

        if not len(log_lines) > 0:
            return False

        log_text = "".join(log_lines)
        return "Process started" in log_text and "Heartbeat" in log_text

    print("\n=== Checking log output ===")
    for process_uuid in process_uuids:
        process_name = process_info[process_uuid]["name"]
        log_file = process_info[process_uuid]["log_file"]

        log_lines_correct = wait_for(
            lambda lf=log_file: check_log_lines(lf),
            True,
            timeout=timeout,
            poll_interval=0.5,
        )

        assert log_lines_correct, f"Log lines verification failed for {process_name}"
        print(f"✓ {process_name} logs verified")


def verify_all_processes_alive(ssh_manager, process_uuids, expected_count):
    """
    Verify that all expected processes are currently alive.

    Args:
        ssh_manager: SSH process lifetime manager instance
        process_uuids: List of process UUIDs to check
        expected_count: Expected number of alive processes

    Raises:
        AssertionError: If alive count doesn't match expected count
    """
    print("\n=== Verifying process status ===")
    alive_count = sum(1 for uuid in process_uuids if ssh_manager.is_process_alive(uuid))
    assert alive_count == expected_count, (
        f"Expected {expected_count} alive, got {alive_count}"
    )
    print(f"✓ All {expected_count} processes confirmed alive")


def verify_exit_codes(exit_codes, process_uuids, process_info=None):
    """
    Verify that all processes terminated with expected exit codes.

    Args:
        exit_codes: Dictionary mapping process UUIDs to their exit codes
        process_uuids: List of process UUIDs to verify
        process_info: Optional dictionary with additional process information for logging

    Raises:
        AssertionError: If any exit code is None or unexpected
    """
    print("\n=== Verifying exit codes ===")
    for process_uuid in process_uuids:
        exit_code = exit_codes.get(process_uuid)
        assert exit_code is not None, f"No exit code for {process_uuid}"

        # Exit code 0 indicates process successfully handled SIGQUIT
        assert exit_code == 0, f"Unexpected exit code {exit_code} for {process_uuid}"

        if process_info and "role" in process_info.get(process_uuid, {}):
            role = process_info[process_uuid]["role"]
            print(f"Process {process_uuid} ({role}): exit code {exit_code}")
        else:
            print(f"Process {process_uuid}: exit code {exit_code}")


def verify_cleanup_complete(ssh_manager):
    """
    Verify that all process resources have been cleaned up.

    Args:
        ssh_manager: SSH process lifetime manager instance

    Raises:
        AssertionError: If any active processes remain
    """
    active_keys = ssh_manager.get_active_process_keys()
    assert len(active_keys) == 0, (
        f"Found {len(active_keys)} active processes after cleanup"
    )


def verify_all_processes_dead(ssh_manager, process_uuids, expected_count):
    """
    Verify that all expected processes have terminated.

    Args:
        ssh_manager: SSH process lifetime manager instance
        process_uuids: List of process UUIDs to check
        expected_count: Expected number of dead processes

    Raises:
        AssertionError: If dead count doesn't match expected count
    """
    dead_count = sum(
        1 for uuid in process_uuids if not ssh_manager.is_process_alive(uuid)
    )
    assert dead_count == expected_count, (
        f"Expected {expected_count} dead, got {dead_count}"
    )
    print(f"✓ All {expected_count} processes terminated")


def boot_processes_and_kill_individually(ssh_manager, test_file_path):
    """
    Execute and verify lifecycle of multiple concurrent SSH processes.

    Tests typical process lifecycle: process execution, log capture, killing processes
    individually and resource cleanup.

    Args:
        ssh_manager: SSH process lifetime manager instance
        test_file_path: Path to test file (for locating simple_process.py)
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        log_dir = Path(temp_dir)

        simple_process_script = test_file_path.parent / "simple_process.py"
        assert simple_process_script.exists(), (
            f"simple_process.py not found at {simple_process_script}"
        )

        num_processes = 3
        process_uuids = []
        process_info = {}

        print(f"\n=== Executing {num_processes} SSH processes ===")
        for i in range(num_processes):
            process_name = f"test_process_{i}"
            log_file = str(log_dir / f"{process_name}.log")
            process_uuid = str(uuid.uuid4())
            process_uuids.append(process_uuid)

            boot_request = create_boot_request(
                process_name=process_name,
                tree_id="this.isan.application",
                log_file=log_file,
                test_file_path=test_file_path,
            )

            ssh_manager.start_process(uuid=process_uuid, boot_request=boot_request)

            process_info[process_uuid] = {
                "name": process_name,
                "log_file": log_file,
            }

            print(f"Executed {process_name} with UUID {process_uuid}")

        verify_all_processes_alive(ssh_manager, process_uuids, num_processes)
        verify_log_output(ssh_manager, process_uuids, process_info)

        exit_codes = {}
        print("\n=== Terminating all processes ===")
        for process_uuid in process_uuids:
            exit_codes[process_uuid] = ssh_manager.kill_process(
                process_uuid, timeout=10.0
            )

        verify_all_processes_dead(ssh_manager, process_uuids, num_processes)
        verify_exit_codes(exit_codes, process_uuids)
        verify_cleanup_complete(ssh_manager)

        print(
            "\n✓ Test passed: All processes executed, logged, and cleaned up successfully"
        )


def boot_processes_and_terminate_all_same_role(ssh_manager, test_file_path):
    """
    Execute multiple SSH processes with identical roles and terminate all simultaneously.

    Tests batch termination of processes sharing the same role, verifying that
    the role-based shutdown mechanism correctly handles multiple processes within
    a single role category.

    Args:
        ssh_manager: SSH process lifetime manager instance
        test_file_path: Path to test file (for locating simple_process.py)
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        log_dir = Path(temp_dir)

        num_processes = 3
        process_uuids = []
        process_info = {}
        role = "application"

        print(f"\n=== Executing {num_processes} SSH processes with role '{role}' ===")
        for i in range(num_processes):
            process_name = f"test_process_same_role_{i}"
            log_file = str(log_dir / f"{process_name}.log")
            process_uuid = str(uuid.uuid4())
            process_uuids.append(process_uuid)

            boot_request = create_boot_request(
                process_name=process_name,
                tree_id="this.isan.application",
                log_file=log_file,
                test_file_path=test_file_path,
            )

            ssh_manager.start_process(uuid=process_uuid, boot_request=boot_request)

            process_info[process_uuid] = {
                "name": process_name,
                "log_file": log_file,
                "role": role,
            }

            print(f"Executed {process_name} with UUID {process_uuid}")

        verify_all_processes_alive(ssh_manager, process_uuids, num_processes)
        verify_log_output(ssh_manager, process_uuids, process_info)

        print(f"\n=== Terminating all processes with role '{role}' ===")
        exit_codes = ssh_manager.kill_processes(
            process_uuids, process_timeouts={uuid: 10.0 for uuid in process_uuids}
        )

        verify_all_processes_dead(ssh_manager, process_uuids, num_processes)
        verify_exit_codes(exit_codes, process_uuids, process_info)
        verify_cleanup_complete(ssh_manager)

        print(
            f"\n✓ Test passed: All {num_processes} processes with role '{role}' "
            "executed, logged, and cleaned up successfully"
        )


def boot_processes_and_terminate_all_different_role(ssh_manager, test_file_path):
    """
    Execute SSH processes with different roles and verify priority-based termination.

    Tests that the role-based shutdown mechanism correctly terminates processes
    in the expected order according to the defined shutdown sequence. Higher-priority
    roles (earlier in shutdown order) are killed before lower-priority roles.

    Args:
        ssh_manager: SSH process lifetime manager instance
        test_file_path: Path to test file (for locating simple_process.py)
    """
    import threading
    import time

    with tempfile.TemporaryDirectory() as temp_dir:
        log_dir = Path(temp_dir)

        # Define processes with different roles based on shutdown order
        # "application" is terminated before "segment-controller"
        process_configs = [
            {
                "name": "test_process_app_1",
                "role": "application",
                "tree_id": "this.isan.application",
            },
            {
                "name": "test_process_app_2",
                "role": "application",
                "tree_id": "this.isan.application",
            },
            {
                "name": "test_process_segment",
                "role": "segment-controller",
                "tree_id": "thisisa.segment-controller",
            },
        ]

        process_uuids = []
        process_info = {}

        print("\n=== Executing processes with different roles ===")
        for config in process_configs:
            process_name = config["name"]
            role = config["role"]
            log_file = str(log_dir / f"{process_name}.log")
            process_uuid = str(uuid.uuid4())
            process_uuids.append(process_uuid)

            boot_request = create_boot_request(
                process_name=process_name,
                tree_id=config["tree_id"],
                log_file=log_file,
                test_file_path=test_file_path,
            )

            ssh_manager.start_process(uuid=process_uuid, boot_request=boot_request)

            process_info[process_uuid] = {
                "name": process_name,
                "role": role,
                "log_file": log_file,
                "termination_time": None,
            }

            print(f"Executed {process_name} with UUID {process_uuid} and role '{role}'")

        verify_all_processes_alive(ssh_manager, process_uuids, len(process_configs))
        verify_log_output(ssh_manager, process_uuids, process_info)

        print("\n=== Terminating all processes (role-based shutdown) ===")

        def monitor_termination(uuid_to_monitor):
            """
            Poll process status until termination, then record timestamp.

            Args:
                uuid_to_monitor: Process UUID to monitor for termination
            """
            while ssh_manager.is_process_alive(uuid_to_monitor):
                time.sleep(0.05)
            process_info[uuid_to_monitor]["termination_time"] = time.time()

        # Start background threads to monitor each process termination time
        monitor_threads = []
        for process_uuid in process_uuids:
            thread = threading.Thread(
                target=monitor_termination,
                args=(process_uuid,),
                daemon=True,
            )
            thread.start()
            monitor_threads.append(thread)

        # Initiate role-based shutdown and record start time
        start_time = time.time()
        exit_codes = ssh_manager.kill_processes(
            process_uuids, process_timeouts={uuid: 10.0 for uuid in process_uuids}
        )

        # Wait for all monitoring threads to complete
        for thread in monitor_threads:
            thread.join(timeout=15.0)

        verify_all_processes_dead(ssh_manager, process_uuids, len(process_configs))
        verify_exit_codes(exit_codes, process_uuids, process_info)

        print("\n=== Verifying termination order ===")
        # Group processes by role for order verification
        app_processes = [
            uuid
            for uuid in process_uuids
            if process_info[uuid]["role"] == "application"
        ]
        segment_processes = [
            uuid
            for uuid in process_uuids
            if process_info[uuid]["role"] == "segment-controller"
        ]

        # Find latest termination time among higher-priority processes
        app_termination_times = [
            process_info[uuid]["termination_time"] for uuid in app_processes
        ]
        latest_app_termination = max(app_termination_times)

        # Find earliest termination time among lower-priority processes
        segment_termination_times = [
            process_info[uuid]["termination_time"] for uuid in segment_processes
        ]
        earliest_segment_termination = min(segment_termination_times)

        print(
            f"Latest 'application' role termination: "
            f"{latest_app_termination - start_time:.3f}s"
        )
        print(
            f"Earliest 'segment-controller' role termination: "
            f"{earliest_segment_termination - start_time:.3f}s"
        )

        # Verify shutdown order: all "application" processes must terminate
        # before any "segment-controller" processes
        assert latest_app_termination <= earliest_segment_termination, (
            f"Application processes should terminate before segment-controller. "
            f"Latest app: {latest_app_termination - start_time:.3f}s, "
            f"Earliest segment: {earliest_segment_termination - start_time:.3f}s"
        )
        print(
            "✓ Role-based termination order verified: "
            "'application' before 'segment-controller'"
        )

        verify_cleanup_complete(ssh_manager)

        print(
            "\n✓ Test passed: Processes with different roles executed, logged, "
            "terminated in correct order, and cleaned up successfully"
        )


@pytest.mark.paramiko
def test_ssh_multi_process_lifecycle_paramiko(ssh_manager_paramiko):
    """
    Test lifecycle of 3 concurrent SSH processes using Paramiko.

    Executes 3 processes via SSH, verifies log output, terminates all
    processes, and confirms complete cleanup.
    """
    boot_processes_and_kill_individually(ssh_manager_paramiko, Path(__file__))


def test_ssh_multi_process_lifecycle_shell(ssh_manager_shell):
    """
    Test lifecycle of 3 concurrent SSH processes using shell.

    Executes 3 processes via SSH, verifies log output, terminates all
    processes, and confirms complete cleanup.
    """
    boot_processes_and_kill_individually(ssh_manager_shell, Path(__file__))


@pytest.mark.paramiko
def test_ssh_terminate_all_same_role_paramiko(ssh_manager_paramiko):
    """
    Test batch termination of processes sharing the same role using Paramiko.

    Executes 3 processes with identical roles via SSH, verifies log output,
    terminates all processes simultaneously, and confirms complete cleanup.
    """
    boot_processes_and_terminate_all_same_role(ssh_manager_paramiko, Path(__file__))


def test_ssh_terminate_all_same_role_shell(ssh_manager_shell):
    """
    Test batch termination of processes sharing the same role using shell.

    Executes 3 processes with identical roles via SSH, verifies log output,
    terminates all processes simultaneously, and confirms complete cleanup.
    """
    boot_processes_and_terminate_all_same_role(ssh_manager_shell, Path(__file__))


@pytest.mark.paramiko
def test_ssh_terminate_all_different_role_paramiko(ssh_manager_paramiko):
    """
    Test priority-based termination of processes with different roles using Paramiko.

    Executes processes with varying role priorities via SSH, verifies log output,
    terminates all processes using role-based shutdown, verifies termination order,
    and confirms complete cleanup.
    """
    boot_processes_and_terminate_all_different_role(
        ssh_manager_paramiko, Path(__file__)
    )


def test_ssh_terminate_all_different_role_shell(ssh_manager_shell):
    """
    Test priority-based termination of processes with different roles using shell.

    Executes processes with varying role priorities via SSH, verifies log output,
    terminates all processes using role-based shutdown, verifies termination order,
    and confirms complete cleanup.
    """
    boot_processes_and_terminate_all_different_role(ssh_manager_shell, Path(__file__))
