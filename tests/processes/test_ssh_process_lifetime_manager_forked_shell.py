import tempfile
import uuid
from pathlib import Path

import pytest

from drunc.grpc_testing_tools.connection_utils import wait_for
from tests.processes.test_ssh_process_lifetime_manager_common import (
    boot_processes_and_kill_individually,
    boot_processes_and_terminate_all_same_role,
    create_boot_request,
    verify_all_processes_alive,
    verify_all_processes_dead,
    verify_cleanup_complete,
    verify_exit_codes,
    verify_log_output,
)


def test_ssh_multi_process_lifecycle_forked(ssh_manager_forked):
    """
    Test lifecycle of 3 concurrent SSH processes using the forked-process manager.

    Executes 3 processes via the child-process-isolated manager, verifies log
    output is reachable from the parent, terminates all processes individually,
    and confirms complete resource cleanup.
    """
    boot_processes_and_kill_individually(ssh_manager_forked, Path(__file__))


def test_ssh_terminate_all_same_role_forked(ssh_manager_forked):
    """
    Test batch termination of processes sharing the same role using the forked manager.

    Executes 3 processes with identical roles, verifies log output, terminates
    all processes simultaneously via kill_processes(), and confirms complete
    resource cleanup.
    """
    boot_processes_and_terminate_all_same_role(ssh_manager_forked, Path(__file__))


def boot_processes_and_terminate_all_different_role_forked(test_file_path):
    """
    Execute SSH processes with different roles via the forked manager and verify
    priority-based termination order.

    Args:
        ssh_manager_forked: Forked SSH process lifetime manager instance
        test_file_path: Path to the test file (used to locate simple_process.py)
    """
    import threading
    import time

    from drunc.processes.ssh_process_lifetime_manager_from_forked_process import (
        SSHProcessLifetimeManagerShellOnForkedProcess,
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        log_dir = Path(temp_dir)

        # Termination times recorded by the on_process_exit callback, which fires
        # inside the child process context and is therefore causally ordered.
        termination_times: dict = {}
        callback_events: dict = {}

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

        # Build a new manager instance with the callback wired up.
        # The callback fires in the parent's callback-dispatcher thread, so
        # time.time() here is still subject to thread scheduling — but because
        # the callback_queue is a FIFO and records are enqueued inside the child
        # in actual termination order, the parent will always process them in
        # the same order. Timestamps assigned sequentially from a FIFO are
        # monotonically ordered by definition.
        callback_lock = threading.Lock()

        def on_exit(cb_uuid: str, exit_code, exception):
            with callback_lock:
                termination_times[cb_uuid] = time.monotonic()
            if cb_uuid in callback_events:
                callback_events[cb_uuid].set()

        manager = SSHProcessLifetimeManagerShellOnForkedProcess(
            disable_localhost_host_key_check=True,
            on_process_exit=on_exit,
        )

        try:
            print("\n=== Executing processes with different roles (forked) ===")
            for config in process_configs:
                process_name = config["name"]
                role = config["role"]
                log_file = str(log_dir / f"{process_name}.log")
                process_uuid = str(uuid.uuid4())
                process_uuids.append(process_uuid)
                callback_events[process_uuid] = threading.Event()

                boot_request = create_boot_request(
                    process_name=process_name,
                    tree_id=config["tree_id"],
                    log_file=log_file,
                    test_file_path=test_file_path,
                )

                manager.start_process(uuid=process_uuid, boot_request=boot_request)

                process_info[process_uuid] = {
                    "name": process_name,
                    "role": role,
                    "log_file": log_file,
                }

                print(
                    f"Executed {process_name} with UUID {process_uuid} "
                    f"and role '{role}'"
                )

            verify_all_processes_alive(manager, process_uuids, len(process_configs))
            verify_log_output(manager, process_uuids, process_info)

            print("\n=== Terminating all processes (role-based shutdown, forked) ===")
            exit_codes = manager.kill_processes(
                process_uuids,
                process_timeouts={u: 10.0 for u in process_uuids},
            )

            # Wait for all exit callbacks to fire before inspecting timestamps.
            for process_uuid in process_uuids:
                fired = callback_events[process_uuid].wait(timeout=15.0)
                assert fired, (
                    f"on_process_exit callback never fired for "
                    f"{process_info[process_uuid]['name']}"
                )

            verify_all_processes_dead(manager, process_uuids, len(process_configs))
            verify_exit_codes(exit_codes, process_uuids, process_info)

            print("\n=== Verifying termination order via callback timestamps ===")

            app_processes = [
                u for u in process_uuids if process_info[u]["role"] == "application"
            ]
            segment_processes = [
                u
                for u in process_uuids
                if process_info[u]["role"] == "segment-controller"
            ]

            latest_app = max(termination_times[u] for u in app_processes)
            earliest_segment = min(termination_times[u] for u in segment_processes)

            print(f"Latest 'application' callback:        {latest_app:.6f}")
            print(f"Earliest 'segment-controller' callback: {earliest_segment:.6f}")

            assert latest_app <= earliest_segment, (
                f"Application processes should terminate before segment-controller "
                f"(delta: {earliest_segment - latest_app:.6f}s)"
            )
            print(
                "✓ Role-based termination order verified via callbacks: "
                "'application' before 'segment-controller'"
            )

            verify_cleanup_complete(manager)

        finally:
            manager.shutdown()


def test_forked_manager_worker_process_is_alive(ssh_manager_forked):
    """
    Verify that the worker child process is alive immediately after construction.

    Ensures that the multiprocessing.Process instance started during __init__
    is running and ready to receive requests before any process management
    calls are made.
    """
    assert ssh_manager_forked._worker.is_alive(), (
        "Worker process should be alive immediately after construction"
    )


def test_forked_manager_shutdown_terminates_worker(ssh_manager_forked):
    """
    Verify that calling shutdown() causes the worker process to exit cleanly.

    Calls shutdown() explicitly (the fixture will call it again harmlessly) and
    asserts the child process is no longer alive, confirming the sentinel-based
    shutdown mechanism works correctly.
    """
    ssh_manager_forked.shutdown()
    ssh_manager_forked._worker.join(timeout=5.0)
    assert not ssh_manager_forked._worker.is_alive(), (
        "Worker process should have exited after shutdown()"
    )


def test_forked_manager_call_after_shutdown_raises(ssh_manager_forked):
    """
    Verify that calling a management method after shutdown() raises RuntimeError.

    After the child process has been shut down, any attempt to forward a method
    call should raise a RuntimeError rather than blocking indefinitely or
    returning a silent failure.
    """
    ssh_manager_forked.shutdown()
    ssh_manager_forked._worker.join(timeout=5.0)

    with pytest.raises(RuntimeError, match="worker process is no longer running"):
        ssh_manager_forked.get_active_process_keys()


def test_forked_manager_on_process_exit_callback(tmp_path):
    """
    Verify that the on_process_exit callback is invoked in the parent process
    when a managed process exits.

    Starts a single process, kills it, and asserts that the callback registered
    on the forked manager receives the correct UUID and a non-None exit code
    within a reasonable timeout.
    """
    import threading

    from drunc.processes.ssh_process_lifetime_manager_from_forked_process import (
        SSHProcessLifetimeManagerShellOnForkedProcess,
    )

    callback_event = threading.Event()
    callback_results = {}

    def on_exit(cb_uuid: str, exit_code, exception):
        callback_results["uuid"] = cb_uuid
        callback_results["exit_code"] = exit_code
        callback_event.set()

    # Build a manager with the callback registered.
    manager = SSHProcessLifetimeManagerShellOnForkedProcess(
        disable_localhost_host_key_check=True,
        on_process_exit=on_exit,
    )

    try:
        process_uuid = str(uuid.uuid4())
        log_file = str(tmp_path / "cb_test.log")

        boot_request = create_boot_request(
            process_name="callback_test_process",
            tree_id="this.isan.application",
            log_file=log_file,
            test_file_path=Path(__file__),
        )

        manager.start_process(uuid=process_uuid, boot_request=boot_request)

        # Wait until the process is alive before killing it.
        process_alive = wait_for(
            lambda: manager.is_process_alive(process_uuid),
            expected_value=True,
            timeout=10.0,
            poll_interval=0.2,
        )
        assert process_alive, "Process should be alive before kill"

        manager.kill_process(process_uuid, timeout=10.0)

        # Allow time for the exit event to propagate from child to parent.
        callback_fired = callback_event.wait(timeout=15.0)

        assert callback_fired, (
            "on_process_exit callback was not invoked within the timeout after killing the process"
        )
        assert callback_results.get("uuid") == process_uuid, (
            f"Callback received wrong UUID: {callback_results.get('uuid')}"
        )
        assert callback_results.get("exit_code") is not None, (
            "Callback should receive a non-None exit code"
        )
    finally:
        manager.shutdown()
