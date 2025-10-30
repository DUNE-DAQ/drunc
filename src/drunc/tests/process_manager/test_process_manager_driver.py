"""
Test suite for ProcessManagerDriver gRPC method invocations.

This module tests that the ProcessManagerDriver correctly invokes the underlying
gRPC stub methods and properly handles gRPC exceptions.

if any of these tests fail it is likely that the driver method implementations
have changed. The tests should be checked to see if they need to be updated
or if a bug was introduced.
"""

from unittest.mock import MagicMock, patch

import grpc
import pytest
from druncschema.process_manager_pb2 import (
    BootRequest,
    ProcessDescription,
    ProcessMetadata,
    ProcessRestriction,
)
from druncschema.token_pb2 import Token

from drunc.connectivity_service.exceptions import ApplicationLookupUnsuccessful
from drunc.exceptions import DruncSetupException, DruncShellException
from drunc.process_manager.process_manager_driver import ProcessManagerDriver


@pytest.fixture(scope="module")
def mock_logger():
    with patch(
        "drunc.process_manager.process_manager_driver.get_logger"
    ) as mock_get_logger:
        mock_logger_instance = MagicMock()
        mock_get_logger.return_value = mock_logger_instance
        yield mock_logger_instance


@pytest.fixture(scope="function")
def mock_driver(mock_logger):
    """
    Create a ProcessManagerDriver instance with a mocked gRPC stub.

    This fixture creates a driver instance where the underlying gRPC channel
    and stub are mocked, allowing tests to verify method invocations without
    requiring a real gRPC server.

    Returns:
        ProcessManagerDriver: Driver instance with mocked dependencies
    """
    with (
        patch("drunc.process_manager.process_manager_driver.grpc.insecure_channel"),
        patch(
            "drunc.process_manager.process_manager_driver.ProcessManagerStub"
        ) as mock_stub_class,
    ):
        # Create mock stub instance that will be returned by ProcessManagerStub()
        mock_stub = MagicMock()
        mock_stub_class.return_value = mock_stub

        # Initialise driver with mocked dependencies
        driver = ProcessManagerDriver(address="localhost:50051", token=Token())

        # Attach mock stub for easy access in tests
        driver._mock_stub = mock_stub

        return driver


@pytest.fixture(scope="function")
def boot_test_setup(mock_driver):
    """
    Fixture to prepare common mocks for testing the `boot` method of a process manager driver.
    """

    def _setup(*, is_ready=True, grpc_error=None):
        # Create a mock boot request with metadata and host restriction
        mock_request = MagicMock()
        mock_request.process_description.metadata.name = "test_app"
        mock_request.process_restriction.allowed_hosts = {"host1"}

        # Create a mock session DAL with no infrastructure applications
        fake_dal = MagicMock(infrastructure_applications=[])

        # Mock connectivity service
        csc_mock = MagicMock(is_ready=MagicMock(return_value=is_ready))
        mock_driver._connect_to_service = MagicMock(
            return_value=(csc_mock, "server", 1234)
        )

        # Internal methods of the driver
        mock_driver._consolidate_config = MagicMock()
        mock_driver._initialise_session = MagicMock(return_value=("db", fake_dal))

        mock_driver._convert_oks_to_boot_request = MagicMock(
            return_value=[mock_request]
        )
        mock_driver._discover_controller = MagicMock()

        # Configure the boot stub to either return a response or raise an error
        if grpc_error:
            mock_driver.stub.boot = MagicMock(side_effect=grpc_error)
        else:
            mock_driver.stub.boot = MagicMock(return_value="boot_response")

        return mock_request, csc_mock

    return _setup


@patch("drunc.process_manager.oks_parser.collect_apps")
@patch("drunc.process_manager.oks_parser.collect_infra_apps")
def test_collect_all_apps_merges_correctly(mock_infra_apps, mock_apps, mock_driver):
    """
    Test that `_collect_all_apps` correctly merges infrastructure apps and DAQ apps.
    """
    mock_session_dal = MagicMock()

    mock_db = MagicMock()
    mock_apps.return_value = [
        {"tree_id": "0.1", "name": "daq_app_1"},
        {"tree_id": "0.2", "name": "daq_app_2"},
    ]
    mock_infra_apps.return_value = [
        {"tree_id": "1.0", "name": "infra_app_1"},
    ]
    result = mock_driver._collect_all_apps(
        oks_conf="config.oks",
        session_dal=mock_session_dal,
        db=mock_db,
        session_name="test_session",
    )

    # Assert the stub wasn't used
    assert mock_driver._mock_stub.method_calls == []
    # Assert the result is a merged list
    assert result == [
        {"tree_id": "1.0", "name": "infra_app_1"},
        {"tree_id": "0.1", "name": "daq_app_1"},
        {"tree_id": "0.2", "name": "daq_app_2"},
    ]


def test_prepare_exec_and_args_with_session_dal_rtse_script(mock_driver):
    """
    Test that `_prepare_exec_and_args` uses `session_dal.rte_script` when it's provided.
    """
    session_dal = MagicMock()
    session_dal.rte_script = "mock_path/daq_app_rte.sh"
    exe = "dummy_executable"
    args = ["--flag", "value"]

    result = mock_driver._prepare_exec_and_args(session_dal, exe, args)

    assert len(result) == 2

    # Check that all items are instances of ProcessDescription.ExecAndArgs
    for item in result:
        assert isinstance(item, ProcessDescription.ExecAndArgs)

    # First command should source session_dal RTE script
    assert result[0].exec == "source"
    assert result[0].args == ["mock_path/daq_app_rte.sh"]

    # Second command should be the original executable with its arguments
    assert result[1].exec == exe
    assert result[1].args == args


@patch("drunc.process_manager.process_manager_driver.get_rte_script")
def test_prepare_exec_and_args_no_session_dal_rte_script(
    mock_get_rte_script, mock_driver
):
    """
    Test that `_prepare_exec_and_args` falls back to `get_rte_script` when session_dal.rte_script` is None.
    Ensures the fallback RTE script is used and the command sequence is correct.
    """

    # Create a mock session_dal object with no rte_script defined
    session_dal = MagicMock()
    session_dal.rte_script = None

    # Mock the fallback get_rte_script function to return a known path
    mock_get_rte_script.return_value = "mock_path_get_rte_script.sh"

    exe = "dummy_executable"
    args = ["--flag", "value"]

    result = mock_driver._prepare_exec_and_args(session_dal, exe, args)

    assert len(result) == 2

    # First command should source the fallback RTE script
    assert result[0].exec == "source"
    assert result[0].args == ["mock_path_get_rte_script.sh"]

    # Second command should be the original executable with its arguments
    assert result[1].exec == exe
    assert result[1].args == args


@patch("drunc.process_manager.process_manager_driver.get_rte_script")
def test_prepare_exec_and_args_no_rte_script(mock_get_rte_script, mock_driver):
    """
    Test that `_prepare_exec_and_args` raises DruncSetupException when no RTE script is found.
    """
    session_dal = MagicMock()
    session_dal.rte_script = None
    mock_get_rte_script.return_value = None
    mock_get_rte_script.side_effect = DruncSetupException("No RTE script found.")

    exe = "dummy_executable"
    args = ["--flag", "value"]

    with pytest.raises(DruncSetupException, match="No RTE script found."):
        mock_driver._prepare_exec_and_args(session_dal, exe, args)


def test_boot_success(mock_driver, boot_test_setup):
    """
    Test that `boot` yields process responses as expected.
    """
    # Simulate connection is ready
    boot_test_setup(is_ready=True)

    # Control timing behaviour
    with patch("time.time", return_value=100), patch("time.sleep") as mock_sleep:
        responses = list(
            mock_driver.boot(
                conf_file="conf.yaml",
                conf_id="conf1",
                user="test_user",
                session_name="test_session",
                log_level="INFO",
            )
        )

    assert responses == ["boot_response"]

    # Confirm that controller discovery was triggered
    mock_driver._discover_controller.assert_called_once()

    # Verify that no sleep was needed between boots
    mock_sleep.assert_not_called()


def test_boot_connectivity_service_not_ready(mock_driver, boot_test_setup):
    """
    Test that `boot` raises DruncSetupException when the connectivity service is not ready.
    """
    # Simulate connection is not ready ready
    boot_test_setup(is_ready=False)

    with pytest.raises(DruncSetupException, match="Connectivity service is not ready"):
        list(
            mock_driver.boot(
                conf_file="conf.yaml",
                conf_id="conf1",
                user="user",
                session_name="session",
                log_level="DEBUG",
            )
        )


def test_boot_handles_grpc_exception(mock_driver, boot_test_setup):
    """
    Test that gRPC exceptions are handled gracefully when boot is called.
    """

    grpc_error = grpc.RpcError("Connection failed")
    boot_test_setup(grpc_error=grpc_error)

    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = grpc_error

        # Expect the exception to be raised after error handling
        with pytest.raises(grpc.RpcError):
            list(
                mock_driver.boot(
                    conf_file="conf.yaml",
                    conf_id="conf1",
                    user="u",
                    session_name="s",
                    log_level="INFO",
                )
            )
        mock_handler.assert_called_once_with(grpc_error)


@patch("drunc.process_manager.process_manager_driver.get_log_path")
@patch("drunc.process_manager.process_manager_driver.copy_token", return_value=Token())
@patch("drunc.process_manager.process_manager_driver.host_is_local", return_value=False)
def test_build_boot_request_success(
    mock_host_is_local,
    mock_copy_token,
    mock_get_log_path,
    mock_driver,
    app_data,
    bootrequest,
    monkeypatch,
):
    """
    Test that `_build_boot_request` correctly constructs a BootRequest with all expected values
    when the host is not local or the log directory exists.
    """
    monkeypatch.setenv("DUNE_DAQ_BASE_RELEASE", "release1")
    monkeypatch.setenv("SPACK_RELEASES_DIR", "spack_release")

    mock_get_log_path.return_value = app_data["log_path"]

    # Mock the _prepare_exec_and_args method on the driver instance
    mock_driver._prepare_exec_and_args = MagicMock(
        return_value=[{"exec": "binary", "args": ["--arg1"]}]
    )

    breq = mock_driver._build_boot_request(
        app=app_data,
        user="test_user",
        session_name="session1",
        session_dal=MagicMock(),
        session_log_path="/log/dir",
        override_logs=False,
        pwd="/pwd",
    )
    assert breq == bootrequest


@patch(
    "drunc.process_manager.process_manager_driver.get_log_path",
    return_value="dummy_path",
)
@patch("drunc.process_manager.process_manager_driver.host_is_local", return_value=True)
@patch("os.path.exists", return_value=False)
def test_build_boot_request_exception(
    mock_exists, mock_host_local, mock_get_log_path, app_data, mock_driver
):
    """
    Test that `_build_boot_request` raises DruncShellException when host is local and log path doesn't exist.
    """
    mock_driver._prepare_exec_and_args = MagicMock(
        return_value=[{"exec": "binary", "args": ["--arg1"]}]
    )

    with pytest.raises(DruncShellException):
        mock_driver._build_boot_request(
            app=app_data,
            user="test_user",
            session_name="session1",
            session_dal=MagicMock(),
            session_log_path="/log/dir",
            override_logs=False,
            pwd="/pwd",
        )


def test_convert_oks_to_boot_request_yields_correct_number(mock_driver, app_data):
    """
    Test that `_convert_oks_to_boot_request` yields one BootRequest per app.
    """

    # Internal methods of the driver
    mock_driver._collect_all_apps = MagicMock()
    mock_driver._build_boot_request = MagicMock()
    mock_driver._collect_all_apps.return_value = [app_data]
    mock_driver._build_boot_request.side_effect = [BootRequest(), BootRequest()]

    result = list(
        mock_driver._convert_oks_to_boot_request(
            oks_conf="config.oks",
            user="test_user",
            session_dal=MagicMock(),
            db=MagicMock(),
            session_name="session1",
            override_logs=False,
        )
    )

    assert len(result) == 1

    # Assert that all items in the result are instances of BootRequest
    assert all(isinstance(b, BootRequest) for b in result)

    assert mock_driver._build_boot_request.call_count == 1


@patch("daqconf.consolidate.consolidate_db")
@patch("drunc.process_manager.process_manager_driver.tempfile.NamedTemporaryFile")
def test_consolidate_config_success(mock_tempfile, mock_consolidate_db, mock_driver):
    """
    Test that `_consolidate_config` calls consolidate_db and returns the temp file name.
    """

    # Simulate a temporary file context manager that returns a known file path
    mock_file = MagicMock()
    mock_file.__enter__.return_value.name = "/tmp/fake.data.xml"
    mock_tempfile.return_value = mock_file

    mock_driver._consolidate_config("session1", "oksconflibs:/path/to/config.oks")

    # Check correct log message
    mock_driver.log.debug.assert_any_call("Validating session1 configuration")

    mock_consolidate_db.assert_called_once_with(
        "/path/to/config.oks", "/tmp/fake.data.xml"
    )


@patch("daqconf.consolidate.consolidate_db", side_effect=Exception("bad config"))
@patch("drunc.process_manager.process_manager_driver.tempfile.NamedTemporaryFile")
def test_consolidate_config_exception(mock_tempfile, mock_consolidate_db, mock_driver):
    """
    Test that `_consolidate_config` logs a critical error and returns None when consolidate_db fails.
    """
    # Simulate a temporary file context manager that returns a known file path
    mock_file = MagicMock()
    mock_file.__enter__.return_value.name = "/tmp/fake.data.xml"
    mock_tempfile.return_value = mock_file

    mock_driver._consolidate_config("session1", "oksconflibs:/invalid/config.oks")
    mock_driver.log.critical.assert_called()

    # Check log message contains expected errors
    args, _ = mock_driver.log.critical.call_args
    assert "Invalid configuration passed" in args[0]
    assert "oks_dump --files-only /invalid/config.oks" in args[0]


@patch("conffwk.Configuration")
def test_initialise_session_success(mock_config_class, mock_driver):
    """
    Test that `_initialise_session` returns the configuration and session DAL objects.
    """
    mock_db = MagicMock()
    mock_session_dal = MagicMock()
    mock_config_class.return_value = mock_db

    mock_db.get_dal.return_value = mock_session_dal
    result = mock_driver._initialise_session("config.oks", "session123")

    mock_config_class.assert_called_once_with("config.oks")
    mock_db.get_dal.assert_called_once_with(class_name="Session", uid="session123")
    assert result == (mock_db, mock_session_dal)


@patch("conffwk.Configuration", side_effect=Exception("bad config"))
def test_initialise_session_config_failure(mock_config_class, mock_driver):
    """
    Test that `_initialise_session` raises an exception if Configuration fails.
    """
    with pytest.raises(Exception, match="bad config"):
        mock_driver._initialise_session("invalid.oks", "session123")


@patch("drunc.process_manager.process_manager_driver.ConnectivityServiceClient")
def test_connect_to_service_success(mock_client_class, mock_driver):
    """
    Test that `_connect_to_service` returns a client and connection details when connectivity_service is present.
    """
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance

    mock_session_dal = MagicMock()
    mock_session_dal.connectivity_service.host = "localhost"
    mock_session_dal.connectivity_service.service.port = 1234

    result = mock_driver._connect_to_service(mock_session_dal, "session1")

    mock_client_class.assert_called_once_with("session1", "localhost:1234")

    assert result == (mock_client_instance, "localhost", 1234)


def test_connect_to_service_none(mock_driver):
    """
    Test that `_connect_to_service` returns (None, None, None) when connectivity service is missing.
    """
    mock_session_dal = MagicMock()
    mock_session_dal.connectivity_service = None

    result = mock_driver._connect_to_service(mock_session_dal, "session1")

    assert result == (None, None, None)


@patch(
    "drunc.process_manager.process_manager_driver.get_control_type_and_uri_from_connectivity_service"
)
@patch(
    "drunc.process_manager.process_manager_driver.get_segment_lookup_timeout",
    return_value=30,
)
@patch("drunc.process_manager.oks_parser.collect_variables")
def test_discover_controller_with_csc_success(
    mock_collect, mock_timeout, mock_get_uri, mock_driver
):
    """
    Test that `_discover_controller` `sets controller_address correctly` when connectivity service is available.
    """
    mock_get_uri.return_value = ("grpc", "grpc://controller:1234")

    mock_driver._discover_controller(
        session_dal=MagicMock(),
        session_name="session1",
        csc=MagicMock(),
        connection_server="localhost",
        connection_port=1234,
    )
    assert mock_driver.controller_address == "controller:1234"
    mock_driver.log.debug.assert_called()


@patch(
    "drunc.process_manager.process_manager_driver.get_segment_lookup_timeout",
    return_value=30,
)
@patch("drunc.process_manager.oks_parser.collect_variables")
def test_discover_controller_with_csc_failure(mock_collect, mock_timeout, mock_driver):
    """
    Test that `_discover_controller` logs failure when controller lookup fails.
    """

    # Create a mock session DAL with a controller name and empty environment
    session_dal = MagicMock()
    session_dal.segment.controller.id = "controller"
    session_dal.environment = {}

    # Mock failure logging method to track its invocation
    mock_driver._log_controller_lookup_failure = MagicMock()

    with patch(
        "drunc.process_manager.process_manager_driver.get_control_type_and_uri_from_connectivity_service"
    ) as exc_handler:
        exc_handler.side_effect = ApplicationLookupUnsuccessful
        mock_driver._discover_controller(
            session_dal=session_dal,
            session_name="session1",
            csc=MagicMock(),
            connection_server="localhost",
            connection_port=1234,
        )
        # Assert that the failure was logged with the correct parameters
        mock_driver._log_controller_lookup_failure.assert_called_once_with(
            "session1", "controller", "localhost", 1234
        )
        # Assert that no controller address was set due to the failure
        assert mock_driver.controller_address is None


@patch(
    "drunc.process_manager.process_manager_driver.resolve_localhost_and_127_ip_to_network_ip",
    return_value="192.168.1.10",
)
@patch("drunc.process_manager.oks_parser.collect_variables")
def test_discover_controller_without_connectivity_service(
    mock_collect, mock_resolve_ip, mock_driver
):
    """
    Test that `_discover_controller` resolves controller address from exposes_service when connectivity is None.
    """
    # Mock service that matches the expected controller service ID
    service = MagicMock()
    service.id = "controller_control"  # this is how `_discover_controller` constructs the id if csc is None
    service.port = 5678
    service.protocol = "grpc"

    # Create a mock controller with the service and host info
    controller = MagicMock()
    controller.id = "controller"
    controller.exposes_service = [service]
    controller.runs_on.runs_on.id = "localhost"

    segment = MagicMock()
    segment.controller = controller
    session_dal = MagicMock()
    session_dal.segment = segment
    session_dal.environment = {}

    # Call the method without any connectivity service
    mock_driver._discover_controller(
        session_dal=session_dal,
        session_name="session1",
        csc=None,
        connection_server="",
        connection_port=0,
    )
    assert mock_driver.controller_address == "192.168.1.10:5678"


@patch("drunc.process_manager.process_manager_driver.copy_token", return_value=Token())
@patch("drunc.process_manager.process_manager_driver.handle_grpc_error")
@patch(
    "drunc.process_manager.process_manager_driver.os.getcwd",
    return_value="/mocked/path",
)
def test_dummy_boot_success(
    mock_getcwd, mock_handle_error, mock_copy_token, mock_driver
):
    """
    Test that `dummy_boot` creates and sends correct BootRequests and yields responses.
    """
    mock_driver.token = Token()

    # Simulate gRPC stub returning two different responses for each process
    mock_driver.stub.boot.side_effect = ["response_0", "response_1"]

    result = list(
        mock_driver.dummy_boot(
            user="test_user",
            session_name="session1",
            n_processes=2,
            sleep=2,
            n_sleeps=2,
            timeout=30,
        )
    )
    assert result == ["response_0", "response_1"]
    assert mock_driver.stub.boot.call_count == 2

    # Assert each BootRequest sent to the stub
    for i, call in enumerate(mock_driver.stub.boot.call_args_list):
        args, _ = call
        request = args[0]
        assert isinstance(request, BootRequest)
        assert request.token == Token()
        assert request.process_description.metadata.name == f"dummy_boot_{i}"
        assert request.process_description.process_execution_directory == "/mocked/path"
        assert request.process_description.process_logs_path.endswith(
            f"dummy-boot_{i}.log"
        )
        assert request.process_restriction.allowed_hosts == ["localhost"]


@patch(
    "drunc.process_manager.process_manager_driver.copy_token", return_value="mock_token"
)
@patch(
    "drunc.process_manager.process_manager_driver.os.getcwd",
    return_value="/mocked/path",
)
def test_dummy_boot_grpc_error_handling(mock_getcwd, mock_copy_token, mock_driver):
    """
    Test that dummy_boot handles grpc.RpcError using handle_grpc_error().
    Simulates a gRPC failure during stub.boot and verifies that the error handler is invoked.
    """
    # Setup mock driver and stub
    mock_driver.token = Token()
    mock_driver._prepare_exec_and_args_dummy_boot = MagicMock(
        return_value=[{"exec": "binary", "args": ["--arg1"]}]
    )
    mock_driver._build_boot_request_dummy_boot = MagicMock()

    # Simulate gRPC error raised by stub.boot
    grpc_error = grpc.RpcError()
    mock_driver.stub.boot = MagicMock(side_effect=grpc_error)
    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = grpc_error
        with pytest.raises(grpc.RpcError):
            list(
                mock_driver.dummy_boot(
                    user="test_user",
                    session_name="test_session",
                    n_processes=1,
                    sleep=1,
                    n_sleeps=1,
                    timeout=30,
                )
            )

        mock_handler.assert_called_once_with(grpc_error)


def test_prepare_exec_and_args_dummy_boot(mock_driver):
    """
    Test that `_prepare_exec_args_dummy_boot` returns the correct sequence of ExecAndArgs
    for a typical input.
    """
    result = mock_driver._prepare_exec_and_args_dummy_boot(sleep=2, n_sleeps=2)

    # Expected sequence:
    # echo "Starting dummy_boot."
    # sleep "2s", echo "4s"
    # sleep "2s", echo "6s"
    # echo "Exiting."
    expected = [
        ("echo", ["Starting dummy_boot."]),
        ("sleep", ["2s"]),
        ("echo", ["2s"]),
        ("sleep", ["2s"]),
        ("echo", ["4s"]),
        ("echo", ["Exiting."]),
    ]

    assert len(result) == len(expected)
    for actual, (exec_name, args) in zip(result, expected):
        assert actual.exec == exec_name
        assert actual.args == args


def test_build_boot_request_dummy_boot_basic(mock_driver):
    """
    Test that `_build_boot_request_dummy_boot` returns a BootRequest with correct metadata,
    execution details, and restrictions.
    """
    user = "test_user"
    session_name = "session1"
    process = 2
    exec_args = []
    pwd = "/mocked/path"
    mock_driver.token = Token()

    request = mock_driver._build_boot_request_dummy_boot(
        user=user,
        session_name=session_name,
        process=process,
        exec_args=exec_args,
        pwd=pwd,
    )

    expected_bootrequest = BootRequest(
        token=Token(),
        process_description=ProcessDescription(
            metadata=ProcessMetadata(
                user=user,
                session=session_name,
                name=f"dummy_boot_{process}",
                hostname="",
            ),
            executable_and_arguments=exec_args,
            env={},
            process_execution_directory=pwd,
            process_logs_path=f"{pwd}/log_{user}_{session_name}_dummy-boot_{process}.log",
        ),
        process_restriction=ProcessRestriction(allowed_hosts=["localhost"]),
    )

    assert request == expected_bootrequest


def test_terminate_success(mock_driver, terminate_response):
    """
    Test that terminate method correctly calls stub.terminate and returns response.

    Verifies that the terminate method creates the correct request, calls the
    underlying gRPC stub, and returns the expected response.
    """
    # Configure mock stub to return expected response
    mock_driver._mock_stub.terminate.return_value = terminate_response

    # Call the method under test
    response = mock_driver.terminate(timeout=30)

    # Verify stub method was called exactly once
    mock_driver._mock_stub.terminate.assert_called_once()

    # Extract the actual call arguments
    call_args = mock_driver._mock_stub.terminate.call_args
    request = call_args[0][0]
    timeout = call_args[1]["timeout"]

    # Verify request structure and timeout parameter
    assert hasattr(request, "token")
    assert timeout == 30
    assert response == terminate_response


def test_terminate_grpc_error(mock_driver):
    """
    Test that terminate method properly handles gRPC exceptions.

    Verifies that when the gRPC stub raises an exception, the driver
    calls the error handling utility function which then re-raises.
    """
    # Configure mock stub to raise gRPC error
    grpc_error = grpc.RpcError("Connection failed")
    mock_driver._mock_stub.terminate.side_effect = grpc_error

    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        # Configure mock handler to re-raise as the real function does
        mock_handler.side_effect = grpc_error

        # Expect the exception to be raised after error handling
        with pytest.raises(grpc.RpcError):
            mock_driver.terminate()

        # Verify error handler was called with the exception
        mock_handler.assert_called_once_with(grpc_error)


def test_kill_success(mock_driver, process_query_request, kill_response):
    """
    Test that kill method correctly calls stub.kill and returns response.
    """
    mock_driver._mock_stub.kill.return_value = kill_response

    response = mock_driver.kill(process_query_request, timeout=45)

    mock_driver._mock_stub.kill.assert_called_once()
    call_args = mock_driver._mock_stub.kill.call_args
    request = call_args[0][0]
    timeout = call_args[1]["timeout"]

    assert request == process_query_request
    assert timeout == 45
    assert response == kill_response


def test_kill_grpc_error(mock_driver, process_query_request):
    """
    Test that kill method properly handles gRPC exceptions.
    """
    grpc_error = grpc.RpcError("Service unavailable")
    mock_driver._mock_stub.kill.side_effect = grpc_error

    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = grpc_error

        with pytest.raises(grpc.RpcError):
            mock_driver.kill(process_query_request)

        mock_handler.assert_called_once_with(grpc_error)


def test_logs_success(mock_driver, log_request, logs_response):
    """
    Test that logs method correctly calls stub.logs and returns response.
    """
    mock_driver._mock_stub.logs.return_value = logs_response

    response = mock_driver.logs(log_request, timeout=20)

    mock_driver._mock_stub.logs.assert_called_once()
    call_args = mock_driver._mock_stub.logs.call_args
    request = call_args[0][0]
    timeout = call_args[1]["timeout"]

    assert request == log_request
    assert timeout == 20
    assert response == logs_response


def test_logs_grpc_error(mock_driver, log_request):
    """
    Test that logs method properly handles gRPC exceptions.
    """
    grpc_error = grpc.RpcError("Authentication failed")
    mock_driver._mock_stub.logs.side_effect = grpc_error

    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = grpc_error

        with pytest.raises(grpc.RpcError):
            mock_driver.logs(log_request)

        mock_handler.assert_called_once_with(grpc_error)


def test_ps_success(mock_driver, process_query_request, ps_response):
    """
    Test that ps method correctly calls stub.ps and returns response.
    """
    mock_driver._mock_stub.ps.return_value = ps_response

    response = mock_driver.ps(process_query_request, timeout=15)

    mock_driver._mock_stub.ps.assert_called_once()
    call_args = mock_driver._mock_stub.ps.call_args
    request = call_args[0][0]
    timeout = call_args[1]["timeout"]

    assert request == process_query_request
    assert timeout == 15
    assert response == ps_response


def test_ps_grpc_error(mock_driver, process_query_request):
    """
    Test that ps method properly handles gRPC exceptions.
    """
    grpc_error = grpc.RpcError("Request timeout")
    mock_driver._mock_stub.ps.side_effect = grpc_error

    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = grpc_error

        with pytest.raises(grpc.RpcError):
            mock_driver.ps(process_query_request)

        mock_handler.assert_called_once_with(grpc_error)


def test_flush_success(mock_driver, process_query_request, flush_response):
    """
    Test that flush method correctly calls stub.flush and returns response.
    """
    mock_driver._mock_stub.flush.return_value = flush_response

    response = mock_driver.flush(process_query_request, timeout=25)

    mock_driver._mock_stub.flush.assert_called_once()
    call_args = mock_driver._mock_stub.flush.call_args
    request = call_args[0][0]
    timeout = call_args[1]["timeout"]

    assert request == process_query_request
    assert timeout == 25
    assert response == flush_response


def test_flush_grpc_error(mock_driver, process_query_request):
    """
    Test that flush method properly handles gRPC exceptions.
    """
    grpc_error = grpc.RpcError("Server error")
    mock_driver._mock_stub.flush.side_effect = grpc_error

    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = grpc_error

        with pytest.raises(grpc.RpcError):
            mock_driver.flush(process_query_request)

        mock_handler.assert_called_once_with(grpc_error)


def test_restart_success(mock_driver, process_query_request, restart_response):
    """
    Test that restart method correctly calls stub.restart and returns response.
    """
    mock_driver._mock_stub.restart.return_value = restart_response

    response = mock_driver.restart(process_query_request, timeout=40)

    mock_driver._mock_stub.restart.assert_called_once()
    call_args = mock_driver._mock_stub.restart.call_args
    request = call_args[0][0]
    timeout = call_args[1]["timeout"]

    assert request == process_query_request
    assert timeout == 40
    assert response == restart_response


def test_restart_grpc_error(mock_driver, process_query_request):
    """
    Test that restart method properly handles gRPC exceptions.
    """
    grpc_error = grpc.RpcError("Network unreachable")
    mock_driver._mock_stub.restart.side_effect = grpc_error

    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = grpc_error

        with pytest.raises(grpc.RpcError):
            mock_driver.restart(process_query_request)

        mock_handler.assert_called_once_with(grpc_error)


def test_describe_success(mock_driver, describe_response):
    """
    Test that describe method correctly calls stub.describe and returns response.
    """
    mock_driver._mock_stub.describe.return_value = describe_response

    response = mock_driver.describe(timeout=10)

    mock_driver._mock_stub.describe.assert_called_once()
    call_args = mock_driver._mock_stub.describe.call_args
    request = call_args[0][0]
    timeout = call_args[1]["timeout"]

    # Describe method creates a generic Request with just a token
    assert hasattr(request, "token")
    assert timeout == 10
    assert response == describe_response


def test_describe_grpc_error(mock_driver):
    """
    Test that describe method properly handles gRPC exceptions.
    """
    grpc_error = grpc.RpcError("Service not found")
    mock_driver._mock_stub.describe.side_effect = grpc_error

    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = grpc_error

        with pytest.raises(grpc.RpcError):
            mock_driver.describe()

        mock_handler.assert_called_once_with(grpc_error)
