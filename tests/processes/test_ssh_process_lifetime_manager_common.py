"""
Common functions to test all process lifetime manager implementations.
"""

import getpass
import os
import tempfile
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from druncschema.process_manager_pb2 import (
    BootRequest,
    ProcessDescription,
    ProcessMetadata,
)

from drunc.processes.connection_utils import wait_for
from drunc.processes.exit_status import ExitStatus, ExitStatusSource


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


@dataclass(frozen=True)
class ExitMessageScenario:
    name: str
    kill_mode: str
    expected_source: ExitStatusSource
    expected_message_fragment: str
    expected_reported_exit_code: Optional[int] = None


def boot_processes_and_verify_exit_state_messages(
    ssh_manager,
    test_file_path,
):
    """
    Boot all exit-status scenarios together and validate emitted status messages.

    This helper starts one process per expected exit-status message variant,
    then applies mixed kill strategies (client signal, remote crash, manager kill)
    and verifies the callback-delivered ExitStatus source and
    get_process_manager_log_message() output for each process.

    Args:
        ssh_manager: SSH process lifetime manager implementation under test
        test_file_path: Path to the test file (used to locate simple_process.py)
    """
    scenarios = [
        ExitMessageScenario(
            name="case_client_sigquit",
            kill_mode="client_sigquit",
            expected_source=ExitStatusSource.CLIENT_MONITORING,
            expected_message_fragment="was terminated unexpectedly with SIGQUIT on the SSH client (SIGHUP on the server)",
            expected_reported_exit_code=None,
        ),
        ExitMessageScenario(
            name="case_client_sigkill",
            kill_mode="client_sigkill",
            expected_source=ExitStatusSource.CLIENT_MONITORING,
            expected_message_fragment="was terminated unexpectedly by a SIGKILL to the SSH client (SIGHUP on the server)",
            expected_reported_exit_code=None,
        ),
        ExitMessageScenario(
            name="case_remote_sigquit",
            kill_mode="remote_sigquit",
            expected_source=ExitStatusSource.REMOTE_MONITORING,
            expected_message_fragment="was terminated unexpectedly through the remote pid",
            expected_reported_exit_code=0,
        ),
        ExitMessageScenario(
            name="case_remote_sigkill",
            kill_mode="remote_sigkill",
            expected_source=ExitStatusSource.REMOTE_MONITORING,
            expected_message_fragment="was terminated unexpectedly through the remote pid",
            expected_reported_exit_code=128 + 9,  # SIGKILL
        ),
        ExitMessageScenario(
            name="case_manual_remote_pid",
            kill_mode="manual_remote_pid",
            expected_source=ExitStatusSource.MANUAL_KILL_THROUGH_REMOTE_PID,
            expected_message_fragment="was terminated by the process manager through the remote pid",
            expected_reported_exit_code=0,
        ),
        ExitMessageScenario(
            name="case_manual_ssh_client",
            kill_mode="manual_ssh_client",
            expected_source=ExitStatusSource.MANUAL_KILL_THROUGH_SSH_CLIENT,
            expected_message_fragment="was terminated with a SIGKILL by the process manager through the SSH client",
            expected_reported_exit_code=None,
        ),
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        log_dir = Path(temp_dir)

        callbacks_lock = threading.Lock()
        callback_events: dict[str, threading.Event] = {}
        callback_statuses: dict[str, ExitStatus] = {}
        callback_messages: dict[str, str] = {}

        process_uuids: list[str] = []
        process_info: dict[str, dict] = {}

        def on_exit(cb_uuid: str, exit_status: Optional[ExitStatus], exception):
            if exit_status is None:
                return
            with callbacks_lock:
                callback_statuses[cb_uuid] = exit_status
                metadata = process_info.get(cb_uuid)
                if metadata is not None:
                    callback_messages[cb_uuid] = (
                        exit_status.get_process_manager_log_message(
                            metadata["name"],
                            metadata["session"],
                            metadata["user"],
                        )
                    )
            event = callback_events.get(cb_uuid)
            if event is not None:
                event.set()

        ssh_manager._on_process_exit = on_exit

        print("\n=== Booting one process per exit-status scenario ===")
        for scenario_config in scenarios:
            process_name = scenario_config.name
            process_uuid = str(uuid.uuid4())
            log_file = str(log_dir / f"{process_name}.log")
            process_uuids.append(process_uuid)
            callback_events[process_uuid] = threading.Event()

            boot_request = create_boot_request(
                process_name=process_name,
                tree_id="this.isan.application",
                log_file=log_file,
                test_file_path=test_file_path,
            )

            ssh_manager.start_process(uuid=process_uuid, boot_request=boot_request)

            process_info[process_uuid] = {
                "name": process_name,
                "session": boot_request.process_description.metadata.session,
                "user": boot_request.process_description.metadata.user,
                "kill_mode": scenario_config.kill_mode,
                "scenario": scenario_config,
                "log_file": log_file,
            }

            print(
                f"Executed {process_name} with UUID {process_uuid} "
                f"(kill mode: {scenario_config.kill_mode})"
            )

        verify_all_processes_alive(ssh_manager, process_uuids, len(scenarios))
        verify_log_output(ssh_manager, process_uuids, process_info)

        pid_snapshots = capture_process_pid_snapshots(ssh_manager, process_uuids)

        # Wait until metadata is available for all scenarios that require remote PID.
        for process_uuid in process_uuids:
            kill_mode = process_info[process_uuid]["kill_mode"]
            if kill_mode in ("client_sigkill", "manual_ssh_client"):
                continue
            metadata_ready = wait_for(
                lambda u=process_uuid: ssh_manager.get_remote_pid(u).successful,
                expected_value=True,
                timeout=10.0,
                poll_interval=0.2,
            )
            assert metadata_ready, (
                f"Remote PID not available for process {process_uuid}"
            )

        print("\n=== Triggering all termination paths concurrently ===")

        def trigger_termination(process_uuid: str):
            kill_mode = process_info[process_uuid]["kill_mode"]

            kill_mode_actions = {
                "client_sigquit": lambda: ssh_manager.kill_process_without_metadata(
                    process_uuid,
                    signal_name="QUIT",
                    as_manual_pm_kill=False,
                    timeout=10.0,
                ),
                "client_sigkill": lambda: ssh_manager.kill_process_without_metadata(
                    process_uuid,
                    as_manual_pm_kill=False,
                    timeout=10.0,
                ),
                "remote_sigkill": lambda: ssh_manager.crash_process(
                    process_uuid, signal="KILL"
                ),
                "remote_sigquit": lambda: ssh_manager.crash_process(
                    process_uuid, signal="QUIT"
                ),
                "manual_remote_pid": lambda: ssh_manager.kill_process(
                    process_uuid, timeout=10.0
                ),
                "manual_ssh_client": lambda: ssh_manager.kill_process_without_metadata(
                    process_uuid,
                    as_manual_pm_kill=True,
                    timeout=10.0,
                ),
            }

            action = kill_mode_actions.get(kill_mode)
            if action is None:
                raise RuntimeError(f"Unhandled kill mode: {kill_mode}")
            action()

        with ThreadPoolExecutor(max_workers=len(process_uuids)) as executor:
            futures = [
                executor.submit(trigger_termination, process_uuid)
                for process_uuid in process_uuids
            ]
            for future in futures:
                future.result()

        print("\n=== Waiting for and validating on_process_exit messages ===")
        for process_uuid in process_uuids:
            callback_fired = callback_events[process_uuid].wait(timeout=15.0)
            assert callback_fired, (
                f"Exit callback did not fire for {process_info[process_uuid]['name']}"
            )

            exit_status = callback_statuses.get(process_uuid)
            assert exit_status is not None, (
                f"No ExitStatus captured for {process_info[process_uuid]['name']}"
            )

            scenario: ExitMessageScenario = process_info[process_uuid]["scenario"]
            emitted_message = callback_messages[process_uuid]

            assert exit_status.get_source() is scenario.expected_source, (
                f"Unexpected source for {process_info[process_uuid]['name']}: "
                f"got {exit_status.get_source()}, expected {scenario.expected_source}"
            )

            assert scenario.expected_message_fragment in emitted_message, (
                f"Unexpected emitted message for {process_info[process_uuid]['name']}: "
                f"{emitted_message}"
            )

            if scenario.expected_reported_exit_code is not None:
                assert (
                    exit_status.get_reported_exit_code()
                    == scenario.expected_reported_exit_code
                ), (
                    f"Unexpected reported exit code for "
                    f"{process_info[process_uuid]['name']}: "
                    f"got {exit_status.get_reported_exit_code()}, expected "
                    f"{scenario.expected_reported_exit_code}"
                )

        # Ensure all resources are cleaned up even for paths that intentionally
        # bypass normal cleanup (e.g. crash_process and direct client signals).
        ssh_manager.kill_all_processes(
            process_timeouts={process_uuid: 10.0 for process_uuid in process_uuids}
        )
        verify_cleanup_complete(ssh_manager, pid_snapshots=pid_snapshots)


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


def verify_exit_codes(
    exit_codes: dict[str, ExitStatus], process_uuids, process_info=None
):
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
        exit_status = exit_codes.get(process_uuid)
        assert exit_status is not None, f"No exit status for {process_uuid}"
        exit_code = exit_status.get_reported_exit_code()

        # Exit code 0 indicates process successfully handled SIGQUIT
        assert exit_code == 0, f"Unexpected exit code {exit_code} for {process_uuid}"

        if process_info and "role" in process_info.get(process_uuid, {}):
            role = process_info[process_uuid]["role"]
            print(f"Process {process_uuid} ({role}): exit code {exit_code}")
        else:
            print(f"Process {process_uuid}: exit code {exit_code}")


def _pid_exists(pid: int) -> bool:
    """Return True if PID exists (including zombie), False otherwise."""
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _pid_state(pid: int) -> Optional[str]:
    """Return /proc state letter for PID, or None when unavailable."""
    try:
        with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as proc_stat:
            fields = proc_stat.read().split()
        if len(fields) >= 3:
            return fields[2]
    except FileNotFoundError:
        return None
    except ProcessLookupError:
        return None
    except PermissionError:
        return None
    return None


def capture_process_pid_snapshots(
    ssh_manager, process_uuids
) -> dict[str, dict[str, int]]:
    """Capture best-effort PID snapshots for processes before kill/cleanup."""
    snapshots: dict[str, dict[str, int]] = {}
    get_runtime_pids = getattr(ssh_manager, "get_runtime_pids", None)

    for process_uuid in process_uuids:
        pid_snapshot: dict[str, int] = {}

        if callable(get_runtime_pids):
            runtime_pids = get_runtime_pids(process_uuid) or {}
            for label, pid in runtime_pids.items():
                if isinstance(pid, int):
                    pid_snapshot[label] = pid

        remote_pid_result = ssh_manager.get_remote_pid(process_uuid)
        if remote_pid_result.successful and isinstance(remote_pid_result.pid, int):
            pid_snapshot["remote_pid"] = remote_pid_result.pid

        snapshots[process_uuid] = pid_snapshot

    return snapshots


def _verify_os_pid_cleanup(
    pid_snapshots: dict[str, dict[str, int]],
    timeout_per_pid: float = 10.0,
) -> None:
    """Verify tracked PIDs fully disappear from the OS and are not zombies."""
    seen_pids: set[int] = set()
    checked_count = 0

    for process_uuid, snapshot in pid_snapshots.items():
        for pid_type, pid in snapshot.items():
            if pid in seen_pids:
                continue
            seen_pids.add(pid)

            cleaned = wait_for(
                lambda p=pid: not _pid_exists(p),
                expected_value=True,
                timeout=timeout_per_pid,
                poll_interval=0.1,
            )
            if cleaned:
                checked_count += 1
                continue

            state = _pid_state(pid)
            if state == "Z":
                raise AssertionError(
                    f"PID {pid} ({pid_type}, process UUID {process_uuid}) still exists as a zombie"
                )

            raise AssertionError(
                f"PID {pid} ({pid_type}, process UUID {process_uuid}) still exists after cleanup "
                f"with state '{state if state is not None else 'unknown'}'"
            )

    if checked_count:
        print(f"✓ OS-level cleanup verified for {checked_count} tracked PID(s)")


def verify_cleanup_complete(ssh_manager, pid_snapshots=None):
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

    if pid_snapshots:
        _verify_os_pid_cleanup(pid_snapshots)


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

        pid_snapshots = capture_process_pid_snapshots(ssh_manager, process_uuids)

        exit_codes = {}
        print("\n=== Terminating all processes ===")
        for process_uuid in process_uuids:
            exit_codes[process_uuid] = ssh_manager.kill_process(
                process_uuid, timeout=10.0
            )

        verify_all_processes_dead(ssh_manager, process_uuids, num_processes)
        verify_exit_codes(exit_codes, process_uuids)
        verify_cleanup_complete(ssh_manager, pid_snapshots=pid_snapshots)

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

        pid_snapshots = capture_process_pid_snapshots(ssh_manager, process_uuids)

        print(f"\n=== Terminating all processes with role '{role}' ===")
        exit_codes = ssh_manager.kill_processes(
            process_uuids, process_timeouts={uuid: 10.0 for uuid in process_uuids}
        )

        verify_all_processes_dead(ssh_manager, process_uuids, num_processes)
        verify_exit_codes(exit_codes, process_uuids, process_info)
        verify_cleanup_complete(ssh_manager, pid_snapshots=pid_snapshots)

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

        pid_snapshots = capture_process_pid_snapshots(ssh_manager, process_uuids)

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

        verify_cleanup_complete(ssh_manager, pid_snapshots=pid_snapshots)

        print(
            "\n✓ Test passed: Processes with different roles executed, logged, "
            "terminated in correct order, and cleaned up successfully"
        )
