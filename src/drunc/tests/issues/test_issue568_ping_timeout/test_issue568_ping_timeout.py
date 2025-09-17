#!/usr/bin/env python3
"""
This is a regressions of tests which use a tree of gRPC processes which mimic
the run control structure. grpc-specific warning and error messages are monitored
e.g. ping_timeout (see issue #568).

The tests are linked to the grpc config so they should flag situations that induce
grpc errors.

"""

import time

import pytest

from drunc.tests.issues.test_issue568_ping_timeout.grpc_connection_tree import (
    GrpcProcessTreeManager,
)


def monitor_for_errors_while_idle(
    tree_manager, total_duration_seconds, check_interval_seconds
):
    """
    Helper function to monitor for errors during an idle period.
    """
    start_time = time.time()
    while (time.time() - start_time) < total_duration_seconds:
        error_found = tree_manager.check_for_errors()
        if error_found is not None:
            return error_found
        time.sleep(check_interval_seconds)
    return None


def test_basic_grpc_tree_communication(capsys):
    """
    Basic test to verify gRPC tree setup, communication, and trace logging.

    This test validates that:
    1. All gRPC servers start correctly and can communicate
    2. Direct client connections work as expected
    3. gRPC trace logging is enabled and producing output in all log files
    """
    with capsys.disabled():
        import os
        from pathlib import Path

        # Enable gRPC logging so output to log files can be checked
        # This should produce 'gRPC Tracers: http' in each log file
        os.environ["GRPC_VERBOSITY"] = "INFO"
        os.environ["GRPC_TRACE"] = "http"

        basic_config = []
        tree_manager = GrpcProcessTreeManager(
            number_of_children=2,
            manager_max_workers=2,
            controller_max_workers=2,
            manager_server_config=basic_config,
            manager_client_config=basic_config,
            root_server_config=basic_config,
            root_client_config=basic_config,
            child_server_config=basic_config,
            child_client_config=basic_config,
        )

        with tree_manager as process_manager:
            # Connect to all servers and perform communication tests
            process_manager.connect_to_all_servers()
            process_manager.perform_full_communication_test()

            # Test direct client to generate additional gRPC traffic
            direct_client = tree_manager.create_direct_client(
                client_id="IdleTestClient", client_options=basic_config
            )
            direct_client.make_request("Initial test from managed DirectRootClient")

            # Verify gRPC http trace logging is working in all log files
            log_files = tree_manager.log_file_manager.get_all_log_files()
            missing_trace_files = []
            for log_file in log_files:
                log_path = Path(log_file)

                # Check if log file exists and is readable
                if not log_path.exists():
                    missing_trace_files.append(f"{log_file} (file does not exist)")
                    continue

                try:
                    # Read log file content
                    with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()

                    # Check for expected gRPC trace output
                    assert "http" in content

                except (IOError, OSError) as e:
                    pytest.fail(f"Error reading log file {log_file}: {e}")

            # Assert that all log files contain the expected trace output
            if missing_trace_files:
                error_msg = (
                    f"gRPC trace logging verification failed for {len(missing_trace_files)} files:\n"
                    + "\n".join(
                        f"  - {file_issue}" for file_issue in missing_trace_files
                    )
                )
                pytest.fail(error_msg)


#######################################################################################
## This tests the live grpc configuration for grpc errors
#######################################################################################


def test_production_grpc_settings_idle(capsys):
    """
    Test current gRPC production settings for grpc errors during idle time
    """
    from drunc.grpc_settings import (
        CONTROLLER_CLIENT_GRPC_CONFIG,
        CONTROLLER_SERVER_GRPC_CONFIG,
        CONTROLLER_SERVER_GRPC_MAX_WORKERS,
        MANAGER_CLIENT_GRPC_CONFIG,
        MANAGER_SERVER_GRPC_CONFIG,
        MANAGER_SERVER_GRPC_MAX_WORKERS,
    )

    with capsys.disabled():
        tree_manager = GrpcProcessTreeManager(
            number_of_children=5,
            manager_max_workers=MANAGER_SERVER_GRPC_MAX_WORKERS,
            controller_max_workers=CONTROLLER_SERVER_GRPC_MAX_WORKERS,
            manager_server_config=MANAGER_SERVER_GRPC_CONFIG,
            manager_client_config=MANAGER_CLIENT_GRPC_CONFIG,
            root_server_config=CONTROLLER_SERVER_GRPC_CONFIG,
            root_client_config=CONTROLLER_CLIENT_GRPC_CONFIG,
            child_server_config=CONTROLLER_SERVER_GRPC_CONFIG,
            child_client_config=CONTROLLER_CLIENT_GRPC_CONFIG,
        )

        with tree_manager as process_manager:
            process_manager.connect_to_all_servers()
            process_manager.perform_full_communication_test()

            error_found = monitor_for_errors_while_idle(
                tree_manager, total_duration_seconds=300, check_interval_seconds=1
            )
            if error_found:
                pytest.fail(
                    f"Error detected too early during idle period with production settings. Grpc behaviour may have changed. Error: {error_found}"
                )

            process_manager.perform_full_communication_test()
            error_found = tree_manager.check_for_errors()
            if error_found:
                pytest.fail(
                    "Error detected after trying to communicate following idle period with production settings."
                )


def test_production_grpc_settings_communicate_with_root_controller_after_idle(
    capsys,
):
    """
    Test that leaving a root controller client idle then making a request
    does not cause a ping_timeout with production gRPC settings.

    """
    from drunc.grpc_settings import (
        CONTROLLER_CLIENT_GRPC_CONFIG,
        CONTROLLER_SERVER_GRPC_CONFIG,
        CONTROLLER_SERVER_GRPC_MAX_WORKERS,
        MANAGER_CLIENT_GRPC_CONFIG,
        MANAGER_SERVER_GRPC_CONFIG,
        MANAGER_SERVER_GRPC_MAX_WORKERS,
    )

    with capsys.disabled():
        keepalive_config = []

        tree_manager = GrpcProcessTreeManager(
            number_of_children=5,
            manager_max_workers=MANAGER_SERVER_GRPC_MAX_WORKERS,
            controller_max_workers=CONTROLLER_SERVER_GRPC_MAX_WORKERS,
            manager_server_config=MANAGER_SERVER_GRPC_CONFIG,
            manager_client_config=MANAGER_CLIENT_GRPC_CONFIG,
            root_server_config=CONTROLLER_SERVER_GRPC_CONFIG,
            root_client_config=CONTROLLER_CLIENT_GRPC_CONFIG,
            child_server_config=CONTROLLER_SERVER_GRPC_CONFIG,
            child_client_config=CONTROLLER_CLIENT_GRPC_CONFIG,
        )

        with tree_manager as process_manager:
            process_manager.connect_to_all_servers()
            process_manager.perform_full_communication_test()

            # connect a separate client to root controller
            direct_client = tree_manager.create_direct_client(
                client_id="IdleTestClient", client_options=keepalive_config
            )
            direct_client.make_request("Initial test from managed DirectRootClient")

            # go idle and monitor for errors during idle period
            error_found = monitor_for_errors_while_idle(
                tree_manager, total_duration_seconds=90, check_interval_seconds=5
            )
            if error_found is not None:
                pytest.fail(
                    f"Ping timeout error detected during idle period with production settings. Error: {error_found}"
                )

            direct_client.make_request("Post-idle test from managed DirectRootClient")

            error_found = tree_manager.check_for_errors()
            if error_found is not None:
                pytest.fail(
                    f"Ping timeout error detected after idle period with production settings. Error: {error_found}"
                )


########################################################################################
## This regression of tests check grpc behaviour against known problematic configurations
## which have previously caused grpc errors
########################################################################################


def test_that_aggressive_client_pinging_during_idle_time_causes_ping_timeout(capsys):
    """
    Test that aggressive client keepalive settings creates ping_timeout message
    from grpc
    """
    manager_server_config = []
    # aggressive pinging from process manager client
    manager_client_config = [("grpc.keepalive_time_ms", 100)]
    root_server_config = []
    root_client_config = []
    child_server_config = []
    child_client_config = []
    manager_max_workers = 2
    controller_max_workers = 2

    with capsys.disabled():
        tree_manager = GrpcProcessTreeManager(
            number_of_children=5,
            manager_max_workers=manager_max_workers,
            controller_max_workers=controller_max_workers,
            manager_server_config=manager_server_config,
            manager_client_config=manager_client_config,
            root_server_config=root_server_config,
            root_client_config=root_client_config,
            child_server_config=child_server_config,
            child_client_config=child_client_config,
        )

        with tree_manager as process_manager:
            process_manager.connect_to_all_servers()
            process_manager.perform_full_communication_test()

            error_found = monitor_for_errors_while_idle(
                tree_manager, total_duration_seconds=300, check_interval_seconds=1
            )
            if error_found:
                return

            process_manager.perform_full_communication_test()
            pytest.fail(
                "No ping timeout errors detected during aggressive keepalive test. Grpc behaviour may have changed."
            )


def test_with_default_settings_after_root_controller_left_idle_causes_ping_timeout(
    capsys,
):
    """
    Test that a root controller client left idle for two minutes
    will create a ping_timeout when all-default gRPC settings are used.
    """
    manager_server_config = []
    manager_client_config = []
    root_server_config = []
    root_client_config = []
    child_server_config = []
    child_client_config = []
    manager_max_workers = 10
    controller_max_workers = 10

    with capsys.disabled():
        keepalive_config = []

        tree_manager = GrpcProcessTreeManager(
            number_of_children=5,
            manager_max_workers=manager_max_workers,
            controller_max_workers=controller_max_workers,
            manager_server_config=manager_server_config,
            manager_client_config=manager_client_config,
            root_server_config=root_server_config,
            root_client_config=root_client_config,
            child_server_config=child_server_config,
            child_client_config=child_client_config,
        )

        with tree_manager as process_manager:
            process_manager.connect_to_all_servers()
            process_manager.perform_full_communication_test()

            # connect a separate client to root controller
            direct_client = tree_manager.create_direct_client(
                client_id="IdleTestClient", client_options=keepalive_config
            )
            direct_client.make_request("Initial test from managed DirectRootClient")

            # go idle and monitor for errors during idle period
            error_found = monitor_for_errors_while_idle(
                tree_manager, total_duration_seconds=90, check_interval_seconds=5
            )
            if error_found is not None:
                pytest.fail(
                    "Ping timeout error detected too early during idle period with default settings. Grpc behaviour may have changed."
                )

            direct_client.make_request("Post-idle test from managed DirectRootClient")

            error_found = tree_manager.check_for_errors()
            if error_found is not None:
                return

            pytest.fail(
                "No ping timeout errors detected after idle period with default settings. Grpc behaviour may have changed."
            )
