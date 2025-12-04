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


def execute_multi_process_lifecycle_test(ssh_manager, test_file_path):
    """
    Execute and verify lifecycle of multiple concurrent SSH processes.

    Tests complete lifecycle: process execution, log capture, termination
    via SIGHUP, and resource cleanup. Verifies that all processes execute
    correctly, produce expected log output, and terminate cleanly.

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
        process_names = []

        print(f"\n=== Executing {num_processes} SSH processes ===")
        for i in range(num_processes):
            process_name = f"test_process_{i}"
            process_names.append(process_name)
            log_file = str(log_dir / f"{process_name}.log")

            process_uuid = str(uuid.uuid4())
            process_uuids.append(process_uuid)

            boot_request = BootRequest(
                process_description=ProcessDescription(
                    metadata=ProcessMetadata(
                        name=process_name,
                        session="test_session",
                        user=getpass.getuser(),
                        hostname="localhost",
                    ),
                    process_execution_directory="/",
                    process_logs_path=log_file,
                )
            )

            boot_request.process_description.executable_and_arguments.add(
                exec=f"python3 {simple_process_script}", args=[process_name]
            )

            ssh_manager.start_process(
                uuid=process_uuid,
                boot_request=boot_request,
            )

            print(f"Executed {process_name} with UUID {process_uuid}")

        print("\n=== Verifying process status ===")
        alive_count = sum(
            1 for uuid in process_uuids if ssh_manager.is_process_alive(uuid)
        )
        assert alive_count == num_processes, (
            f"Expected {num_processes} alive, got {alive_count}"
        )
        print(f"✓ All {num_processes} processes confirmed alive")

        def check_log_lines():
            log_lines = ssh_manager.read_log_file(
                hostname="localhost",
                user=getpass.getuser(),
                log_file=log_file,
                num_lines=100,
            )

            if not len(log_lines) > 0:
                return False

            log_text = "".join(log_lines)
            if "Process started" not in log_text:
                return False
            if "Heartbeat" not in log_text:
                return False

            print(f"✓ {process_name} logs verified ({len(log_lines)} lines)")
            return True

        print("\n=== Checking log output ===")
        for i, process_uuid in enumerate(process_uuids):
            process_name = process_names[i]
            log_file = str(log_dir / f"{process_name}.log")
            log_lines_correct = wait_for(
                check_log_lines, True, timeout=10.0, poll_interval=0.5
            )
            assert log_lines_correct, (
                f"Log lines verification failed for {process_name}"
            )

        exit_codes = {}

        print("\n=== Terminating all processes ===")
        for process_uuid in process_uuids:
            exit_codes[process_uuid] = ssh_manager.kill_process(
                process_uuid, timeout=10.0
            )

        dead_count = sum(
            1 for uuid in process_uuids if not ssh_manager.is_process_alive(uuid)
        )
        assert dead_count == num_processes, (
            f"Expected {num_processes} dead, got {dead_count}"
        )
        print(f"✓ All {num_processes} processes terminated")

        print("\n=== Verifying exit codes ===")
        for process_uuid in process_uuids:
            exit_code = exit_codes[process_uuid]
            assert exit_code is not None, (
                f"No exit code for {process_uuid} after 5s timeout"
            )
            # 143 is the standard unhandled SIGTERM exit code
            assert exit_code == 143, (
                f"Unexpected exit code {exit_code} for {process_uuid}"
            )
            print(f"Process {process_uuid}: exit code {exit_code}")

        active_keys = ssh_manager.get_active_process_keys()
        assert len(active_keys) == 0, (
            f"Found {len(active_keys)} active processes after cleanup"
        )

        print(
            "\n✓ Test passed: All processes executed, logged, and cleaned up successfully"
        )


@pytest.mark.paramiko
def test_ssh_multi_process_lifecycle_paramiko(ssh_manager_paramiko):
    """
    Test lifecycle of 3 concurrent SSH processes using Paramiko.

    Executes 3 processes via SSH, verifies log output, terminates all
    processes, and confirms complete cleanup.
    """
    execute_multi_process_lifecycle_test(ssh_manager_paramiko, Path(__file__))


def test_ssh_multi_process_lifecycle_shell(ssh_manager_shell):
    """
    Test lifecycle of 3 concurrent SSH processes using shell.

    Executes 3 processes via SSH, verifies log output, terminates all
    processes, and confirms complete cleanup.
    """
    execute_multi_process_lifecycle_test(ssh_manager_shell, Path(__file__))
