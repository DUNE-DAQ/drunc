"""
Test SSH Connection Manager with multiple concurrent processes.

Verifies that:
1. SSHConnectionManager can execute multiple remote processes concurrently
2. Log output from each process is captured correctly via remote log files
3. All processes can be terminated cleanly via SIGHUP
4. Resource cleanup is complete
"""

import logging
import os
import tempfile
import time
from pathlib import Path

import pytest
from druncschema.process_manager_pb2 import (
    BootRequest,
    ProcessDescription,
    ProcessMetadata,
)

from drunc.ssh.ssh_connection_manager import SSHConnectionManager


@pytest.fixture
def ssh_manager():
    """Fixture providing SSH Connection Manager with cleanup."""
    logger = logging.getLogger("test_ssh")
    logger.setLevel(logging.DEBUG)

    manager = SSHConnectionManager(
        disable_localhost_host_key_check=True,
        disable_host_key_check=False,
        logger=logger,
    )

    yield manager

    # Guaranteed cleanup
    manager.cleanup_all()


def test_ssh_multi_process_lifecycle(ssh_manager):
    """
    Test lifecycle of 10 concurrent SSH processes.

    Executes 10 processes via SSH, verifies log output, terminates all
    processes via SIGHUP, and confirms complete cleanup.
    """

    # Create temporary directory for log files
    with tempfile.TemporaryDirectory() as temp_dir:
        log_dir = Path(temp_dir)

        # Path to simple_process.py (assumed to be in same directory as test)
        simple_process_script = Path(__file__).parent / "simple_process.py"
        assert simple_process_script.exists(), (
            f"simple_process.py not found at {simple_process_script}"
        )

        # Execute 10 processes
        num_processes = 10
        process_uuids = []
        process_names = []

        print(f"\n=== Executing {num_processes} SSH processes ===")

        for i in range(num_processes):
            process_name = f"test_process_{i}"
            process_names.append(process_name)
            log_file = str(log_dir / f"{process_name}.log")

            # Generate UUID for this process
            import uuid

            process_uuid = str(uuid.uuid4())
            process_uuids.append(process_uuid)

            # Build boot request for SSHConnectionManager
            boot_request = BootRequest(
                process_description=ProcessDescription(
                    metadata=ProcessMetadata(
                        name=process_name,
                        session="test_session",
                        user=os.getenv("USER"),
                        hostname="localhost",
                    ),
                    process_execution_directory="/",
                )
            )

            # Build command to execute
            command = f"python3 {simple_process_script} {process_name}"

            # Execute via SSH connection manager
            ssh_manager.execute_ssh_command(
                uuid=process_uuid,
                boot_request=boot_request,
                hostname="localhost",
                user=os.getenv("USER"),
                command=command,
                log_file=log_file,
                env_vars={},
            )

            print(f"Executed {process_name} with UUID {process_uuid}")

        # Wait for processes to write log entries
        print("\n=== Waiting for log output ===")
        time.sleep(5.0)

        # Verify all processes are alive
        print("\n=== Verifying process status ===")
        alive_count = sum(
            1 for uuid in process_uuids if ssh_manager.is_process_alive(uuid)
        )
        assert alive_count == num_processes, (
            f"Expected {num_processes} alive, got {alive_count}"
        )
        print(f"✓ All {num_processes} processes confirmed alive")

        # Read and verify logs from each process
        print("\n=== Checking log output ===")

        for i, process_uuid in enumerate(process_uuids):
            process_name = process_names[i]
            log_file = str(log_dir / f"{process_name}.log")

            # Read log file via SSH
            log_lines = ssh_manager.read_remote_log_file(
                hostname="localhost",
                user=os.getenv("USER"),
                log_file=log_file,
                num_lines=100,
            )

            assert len(log_lines) > 0, f"No logs found for {process_name}"

            # Verify expected log content
            log_text = "".join(log_lines)
            assert "Process started" in log_text, (
                f"Missing start message in {process_name}"
            )
            assert "Heartbeat" in log_text, f"Missing heartbeat in {process_name}"

            print(f"✓ {process_name} logs verified ({len(log_lines)} lines)")

        # Terminate all processes via SIGHUP (closing SSH connection)
        print("\n=== Terminating all processes via SIGHUP ===")

        for process_uuid in process_uuids:
            ssh_manager.terminate_process(process_uuid, timeout=10.0)

        # Wait for processes to terminate
        time.sleep(2.0)

        # Verify all processes are dead
        dead_count = sum(
            1 for uuid in process_uuids if not ssh_manager.is_process_alive(uuid)
        )
        assert dead_count == num_processes, (
            f"Expected {num_processes} dead, got {dead_count}"
        )
        print(f"✓ All {num_processes} processes terminated")

        # Verify exit codes are captured
        print("\n=== Verifying exit codes ===")
        for process_uuid in process_uuids:
            exit_code = ssh_manager.get_exit_code(process_uuid)
            # Exit code should be -1 for SIGHUP termination
            assert exit_code is not None, f"No exit code for {process_uuid}"
            print(f"Process {process_uuid}: exit code {exit_code}")

        # Clean up all processes
        print("\n=== Cleaning up resources ===")
        for process_uuid in process_uuids:
            ssh_manager.cleanup_process(process_uuid)

        # Verify no active processes remain
        active_keys = ssh_manager.get_active_process_keys()
        assert len(active_keys) == 0, (
            f"Found {len(active_keys)} active processes after cleanup"
        )

        print(
            "\n✓ Test passed: All processes executed, logged, and cleaned up successfully"
        )
