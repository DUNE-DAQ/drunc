#!/usr/bin/env python3
"""

This regression of tests check grpc behaviour against known problematic configurations
which have previously caused grpc errors in certain grpc versions

"""

import time

import pytest

from drunc.tests.grpc.grpc_connection_tree import GrpcProcessTreeManager

# amount of seconds to recreate specific issue with ping_timeout
IDLE_TIME_REQUIRED_FOR_PING_TIMEOUT_TO_OCCUR = 120


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
            return error_found, (time.time() - start_time) 
        time.sleep(check_interval_seconds)
    return None, (time.time() - start_time) 


# This test recreates issue #568 if run in grpc versions 1.68-1.73
@pytest.mark.skip(reason="Not enabled in CI - Use for isolating grpc issues")
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

            error_found, time_elapsed = monitor_for_errors_while_idle(
                tree_manager, total_duration_seconds=300, check_interval_seconds=1
            )
            if error_found:
                return

            process_manager.perform_full_communication_test()
            pytest.fail(
                "No ping timeout errors detected during aggressive keepalive test. Grpc behaviour may have changed."
            )
# This test recreates issue #505 if run in grpc versions 1.68-1.73
@pytest.mark.skip(reason="Not enabled in CI - Use for isolating grpc issues")
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
            error_found, time_elapsed = monitor_for_errors_while_idle(
                tree_manager, total_duration_seconds=IDLE_TIME_REQUIRED_FOR_PING_TIMEOUT_TO_OCCUR, check_interval_seconds=5
            )
            if error_found is not None:
                pytest.fail(
                    f"Ping timeout error detected too early during idle period of {time_elapsed} seconds with default settings. Grpc behaviour may have changed."
                )

            direct_client.make_request("Post-idle test from managed DirectRootClient")

            error_found = tree_manager.check_for_errors()
            if error_found is not None:
                return

            pytest.fail(
                f"No ping timeout errors detected after direct client request following the idle period with default settings. Grpc behaviour may have changed."
            )
