"""Test rich error handling with a real gRPC server with RichErrorServerInterceptor
and a real client stub with RichErrorClientInterceptor.

These tests check:
- server-side exception mapping to gRPC status and rich details
- client-interceptor handling by asserting that `extract_grpc_rich_error` and
the interceptor logger are called.
"""

from concurrent import futures
from unittest.mock import MagicMock, patch

import grpc
import pytest
from druncschema.process_manager_pb2_grpc import (
    ProcessManagerStub,
    add_ProcessManagerServicer_to_server,
)

from drunc.utils.grpc_utils import (
    RichErrorClientInterceptor,
    RichErrorServerInterceptor,
    extract_grpc_rich_error,
)
from tests.process_manager.process_manager_mock_impls import (
    ConcreteProcessManager,
)


class ProcessManagerRichErrorTestSuite:
    """Test suite for rich error message propagation for Process Manager."""

    def __init__(self):
        self.server_port = "50051"
        self.server_address = f"localhost:{self.server_port}"
        self.server = None
        self.channel = None
        self.stub = None
        self.servicer = None
        self.mock_client_logger = None

    def setup_server_and_client(self):
        """
        Initialise a real gRPC server and client for testing rich error handling.

        Args:
            method_name: Name of the method to mock (e.g., 'boot', 'kill')
            mock_response: The response object to return from the mocked method
        """
        # Create a mock servicer for testing serialisation only
        # Configure and start the gRPC server

        with patch("drunc.process_manager.process_manager.get_logger") as mock_logger:
            mock_logger_instance = MagicMock()
            mock_logger.return_value = mock_logger_instance
            mock_conf = MagicMock()
            mock_conf.get_data_type_name.return_value = "dummy"

            self.servicer = ConcreteProcessManager(
                name="dummy_name", configuration=mock_conf
            )

        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            interceptors=[RichErrorServerInterceptor()],
        )
        add_ProcessManagerServicer_to_server(self.servicer, self.server)
        listen_addr = f"[::]:{self.server_port}"
        self.server.add_insecure_port(listen_addr)
        self.server.start()

        # Create a mock logger for the client interceptor
        self.mock_client_logger = MagicMock()
        client_interceptor = RichErrorClientInterceptor(logger=self.mock_client_logger)

        # Create client channel and stub
        raw_channel = grpc.insecure_channel(self.server_address)
        self.channel = grpc.intercept_channel(raw_channel, client_interceptor)
        self.stub = ProcessManagerStub(self.channel)

    def teardown_server_and_client(self):
        """Clean up gRPC server and client resources."""
        if self.channel:
            self.channel.close()
        if self.server:
            self.server.stop(grace=0)
        self.stub = None
        self.servicer = None


@pytest.fixture(scope="function")
def ers_env(monkeypatch):
    """Provide required ERS environment variables for ProcessManager initialisation."""
    monkeypatch.setenv(
        "DUNEDAQ_ERS_ERROR",
        "lstdout",
    )
    monkeypatch.setenv(
        "DUNEDAQ_ERS_FATAL",
        "lstdout",
    )
    monkeypatch.setenv(
        "DUNEDAQ_ERS_INFO",
        "lstdout",
    )
    monkeypatch.setenv(
        "DUNEDAQ_ERS_WARNING",
        "lstdout",
    )


@pytest.fixture(scope="function")
def process_manager_rich_error_test_suite():
    """
    Pytest fixture for ProcessManagerRichErrorTestSuite.

    Yields:
        ProcessManagerSerialisationTestSuite: Test suite instance with server/client lifecycle management
    """
    suite = ProcessManagerRichErrorTestSuite()
    yield suite
    suite.teardown_server_and_client()


@pytest.fixture(scope="function")
def request_by_method(
    boot_request, process_query_request, generic_request, log_request
):
    return {
        "boot": boot_request,
        "restart": process_query_request,
        "kill": process_query_request,
        "terminate": generic_request,
        "ps": process_query_request,
        "logs": log_request,
        "flush": process_query_request,
    }


METHODS_WITH_REQUEST = [
    ("boot", "_boot_impl"),
    ("restart", "_restart_impl"),
    ("kill", "_kill_impl"),
    ("ps", "_ps_impl"),
    ("logs", "_logs_impl"),
    ("flush", "_flush_impl"),
]

METHODS_WITHOUT_REQUEST = [
    ("terminate", "_terminate_impl"),
]


@pytest.mark.parametrize(
    "method_name, impl_name",
    METHODS_WITH_REQUEST,
)
def test_methods_with_request_not_implemented(
    process_manager_rich_error_test_suite,
    ers_env,
    request_by_method,
    method_name,
    impl_name,
):
    """
    Test that methods correctly handle NotImplementedError by returning a Rich Error.
    """

    # Setup the test suite
    process_manager_rich_error_test_suite.setup_server_and_client()

    # Mock the specific implementation method
    mock_impl = MagicMock(side_effect=NotImplementedError())
    setattr(process_manager_rich_error_test_suite.servicer, impl_name, mock_impl)
    request = request_by_method[method_name]

    # Call the method via the stub
    stub_method = getattr(process_manager_rich_error_test_suite.stub, method_name)

    # Patch but allow the real extract_grpc_rich_error to be called as this only happens
    # when the ClientInterceptor catches the RpcError
    with patch(
        "drunc.utils.grpc_utils.extract_grpc_rich_error",
        wraps=extract_grpc_rich_error,
    ) as mock_extract_grpc_rich_error:
        with pytest.raises(grpc.RpcError) as exc_info:
            stub_method(request)

    mock_impl.assert_called_once_with(request)

    err = exc_info.value
    assert err.code() == grpc.StatusCode.UNIMPLEMENTED

    assert "Implementation missing" in err.details()

    # Unpack rich error metadata
    rich_error = extract_grpc_rich_error(err)
    error_info = rich_error.details[0]

    assert error_info is not None
    assert rich_error.code == "UNIMPLEMENTED"
    assert error_info.reason == "NOT_IMPLEMENTED"
    assert error_info.domain == f"ProcessManager.{method_name}"
    mock_extract_grpc_rich_error.assert_called_once()
    process_manager_rich_error_test_suite.mock_client_logger.error.assert_called_once()


@pytest.mark.parametrize(
    "method_name, impl_name",
    METHODS_WITHOUT_REQUEST,
)
def test_methods_without_request_not_implemented(
    process_manager_rich_error_test_suite,
    ers_env,
    request_by_method,
    method_name,
    impl_name,
):
    """
    Check that methods for which the implementation takes no request argument
    return rich errors when NotImplementedError is raised.
    """

    process_manager_rich_error_test_suite.setup_server_and_client()

    mock_impl = MagicMock(side_effect=NotImplementedError())
    setattr(process_manager_rich_error_test_suite.servicer, impl_name, mock_impl)
    request = request_by_method[method_name]

    stub_method = getattr(process_manager_rich_error_test_suite.stub, method_name)

    with patch(
        "drunc.utils.grpc_utils.extract_grpc_rich_error",
        wraps=extract_grpc_rich_error,
    ) as mock_extract_grpc_rich_error:
        with pytest.raises(grpc.RpcError) as exc_info:
            stub_method(request)

    mock_impl.assert_called_once_with()

    err = exc_info.value
    assert err.code() == grpc.StatusCode.UNIMPLEMENTED
    assert "Implementation missing" in err.details()

    rich_error = extract_grpc_rich_error(err)
    error_info = rich_error.details[0]

    assert error_info is not None
    assert rich_error.code == "UNIMPLEMENTED"
    assert error_info.reason == "NOT_IMPLEMENTED"
    assert error_info.domain == f"ProcessManager.{method_name}"
    mock_extract_grpc_rich_error.assert_called_once()
    process_manager_rich_error_test_suite.mock_client_logger.error.assert_called_once()


@pytest.mark.parametrize(
    "method_name, impl_name",
    METHODS_WITH_REQUEST,
)
def test_methods_with_request_unhandled_exception(
    process_manager_rich_error_test_suite,
    ers_env,
    request_by_method,
    method_name,
    impl_name,
):
    """
    Check that methods handle DruncCommandExceptions by returning an I
    NTERNAL error with ErrorInfo.
    """

    # Setup the test suite
    process_manager_rich_error_test_suite.setup_server_and_client()

    # Mock the specific implementation method
    exception_msg = f"Unexpected error in {method_name}"
    mock_impl = MagicMock(side_effect=ValueError(exception_msg))
    setattr(process_manager_rich_error_test_suite.servicer, impl_name, mock_impl)
    request = request_by_method[method_name]

    # Call the method via the stub
    stub_method = getattr(process_manager_rich_error_test_suite.stub, method_name)

    with patch(
        "drunc.utils.grpc_utils.extract_grpc_rich_error",
        wraps=extract_grpc_rich_error,
    ) as mock_extract_grpc_rich_error:
        with pytest.raises(grpc.RpcError) as exc_info:
            stub_method(request)

    mock_impl.assert_called_once_with(request)

    err = exc_info.value
    err_msg = f"Unhandled exception in ProcessManager.{method_name}"
    assert err.code() == grpc.StatusCode.INTERNAL

    assert err_msg in err.details()
    assert exception_msg in err.details()

    # Unpack rich error metadata
    rich_error = extract_grpc_rich_error(err)
    error_info = rich_error.details[0]

    assert error_info is not None
    assert rich_error.code == "INTERNAL"
    assert error_info.reason == "COMMAND_ERROR"
    assert error_info.domain == f"ProcessManager.{method_name}"
    mock_extract_grpc_rich_error.assert_called_once()
    process_manager_rich_error_test_suite.mock_client_logger.error.assert_called_once()


@pytest.mark.parametrize(
    "method_name, impl_name",
    METHODS_WITHOUT_REQUEST,
)
def test_methods_without_request_unhandled_exception(
    process_manager_rich_error_test_suite,
    ers_env,
    request_by_method,
    method_name,
    impl_name,
):
    """
    Check that the methods for which the implementation
    takes no request argument return INTERNAL rich errors for unhandled exceptions.
    """

    process_manager_rich_error_test_suite.setup_server_and_client()

    exception_msg = f"Unexpected error in {method_name}"
    mock_impl = MagicMock(side_effect=ValueError(exception_msg))
    setattr(process_manager_rich_error_test_suite.servicer, impl_name, mock_impl)
    request = request_by_method[method_name]

    stub_method = getattr(process_manager_rich_error_test_suite.stub, method_name)

    with patch(
        "drunc.utils.grpc_utils.extract_grpc_rich_error",
        wraps=extract_grpc_rich_error,
    ) as mock_extract_grpc_rich_error:
        with pytest.raises(grpc.RpcError) as exc_info:
            stub_method(request)

    mock_impl.assert_called_once_with()

    err = exc_info.value
    err_msg = f"Unhandled exception in ProcessManager.{method_name}"
    assert err.code() == grpc.StatusCode.INTERNAL
    assert err_msg in err.details()
    assert exception_msg in err.details()

    rich_error = extract_grpc_rich_error(err)
    error_info = rich_error.details[0]

    assert error_info is not None
    assert rich_error.code == "INTERNAL"
    assert error_info.reason == "COMMAND_ERROR"
    assert error_info.domain == f"ProcessManager.{method_name}"
    mock_extract_grpc_rich_error.assert_called_once()
    process_manager_rich_error_test_suite.mock_client_logger.error.assert_called_once()
