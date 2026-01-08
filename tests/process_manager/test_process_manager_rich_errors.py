
from concurrent import futures
from unittest.mock import MagicMock, patch
from urllib import response

from google.rpc import status_pb2, error_details_pb2, code_pb2
import grpc
import pytest
from druncschema.process_manager_pb2_grpc import (
    ProcessManagerStub,
    add_ProcessManagerServicer_to_server,
)
from drunc.utils.grpc_utils import RichErrorServerInterceptor

from tests.session_manager.conftest import generic_request
from drunc.process_manager.process_manager import ProcessManager

from tests.process_manager.process_manager_mock_impls import (
    ConcreteProcessManager,
)
from drunc.exceptions import DruncCommandException, DruncException


def get_rich_error(rpc_error, detail_type):
    """
    Extracts a specific Rich Error (like ErrorInfo or Help) from a gRPC exception.
    """
    status_proto = status_pb2.Status()
    # Iterate over the trailing metadata to find the binary error details
    for key, value in rpc_error.trailing_metadata():
        if key == "grpc-status-details-bin":
            status_proto.ParseFromString(value)
            # Iterate over the packed messages in the Status
            for detail in status_proto.details:
                if detail.Is(detail_type.DESCRIPTOR):
                    unpacked = detail_type()
                    detail.Unpack(unpacked)
                    return unpacked
    return None

class ProcessManagerRichErrorTestSuite:
    """Test suite for rich error message propagation for Process Manager."""

    def __init__(self):
        self.server_port = "50051"
        self.server_address = f"localhost:{self.server_port}"
        self.server = None
        self.channel = None
        self.stub = None
        self.servicer = None

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
            
            # Pass the mock object instead of '{}'
            self.servicer = ConcreteProcessManager(name="dummy_name", configuration=mock_conf)

        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            interceptors=[RichErrorServerInterceptor()],
            )
        add_ProcessManagerServicer_to_server(self.servicer, self.server)
        listen_addr = f"[::]:{self.server_port}"
        self.server.add_insecure_port(listen_addr)
        self.server.start()

        # Create client channel and stub
        self.channel = grpc.insecure_channel(self.server_address)
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
def process_manager_rich_error_test_suite():
    """
    Pytest fixture for ProcessManagerRichErrorTestSuite.

    Yields:
        ProcessManagerSerialisationTestSuite: Test suite instance with server/client lifecycle management
    """
    suite = ProcessManagerRichErrorTestSuite()
    yield suite
    suite.teardown_server_and_client()


def test_boot_drunc_command_exception(
        process_manager_rich_error_test_suite, boot_request, boot_response):
    """Test boot endpoint serialisation/deserialisation."""
    process_manager_rich_error_test_suite.setup_server_and_client()
    stub = process_manager_rich_error_test_suite.stub
    servicer = process_manager_rich_error_test_suite.servicer

    exc_msg = "Mock internal exception"
    servicer._boot_impl = MagicMock(side_effect=ValueError(exc_msg))
    
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.boot(boot_request)
    
    err = exc_info.value
    assert err.code() == grpc.StatusCode.INTERNAL
    assert exc_msg in err.details()

    error_info = get_rich_error(err, error_details_pb2.ErrorInfo)
    
    assert error_info is not None, "Metadata missing ErrorInfo"
    assert error_info.domain == "ProcessManager.boot"
    assert error_info.reason == "UNHANDLED_EXCEPTION"
    assert error_info.metadata["original_error"] == exc_msg

def test_boot_not_implemented(
        process_manager_rich_error_test_suite, boot_request):
    """Test boot endpoint correctly handles NotImplementedError."""
    
    process_manager_rich_error_test_suite.setup_server_and_client()
    stub = process_manager_rich_error_test_suite.stub
    servicer = process_manager_rich_error_test_suite.servicer

    servicer._boot_impl = MagicMock(side_effect=NotImplementedError())
    
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.boot(boot_request)
    
    err = exc_info.value

    assert err.code() == grpc.StatusCode.UNIMPLEMENTED
    assert "Implementation missing in ProcessManager.boot" in err.details()

    help_info = get_rich_error(err, error_details_pb2.Help)    
    assert help_info.links[0].description == "Check Documentation"
    assert "github.com/dune-daq/drunc/issues" in help_info.links[0].url


@pytest.mark.parametrize(
    "method_name, impl_name",
    [
        ("boot", "_boot_impl"),
        ("restart", "_restart_impl"),
        ("kill", "_kill_impl"),
        ("terminate", "_terminate_impl"),
        ("ps", "_ps_impl"),
        ("logs", "_logs_impl"),
    ]
)
def test_all_methods_not_implemented(
    process_manager_rich_error_test_suite, 
    method_name, 
    impl_name, 
    boot_request):
    """
    Parametrized test to verify that all ProcessManager methods correctly 
    handle NotImplementedError by returning a Rich Error.
    """
    
    # Setup the test suite
    process_manager_rich_error_test_suite.setup_server_and_client()
    
    # Mock the specific implementation method
    mock_impl = MagicMock(side_effect=NotImplementedError())
    setattr(process_manager_rich_error_test_suite.servicer, impl_name, mock_impl)

    # Call the method via the stub
    stub_method = getattr(process_manager_rich_error_test_suite.stub, method_name)
    
    with pytest.raises(grpc.RpcError) as exc_info:
        stub_method(boot_request)
    
    err = exc_info.value
    assert err.code() == grpc.StatusCode.UNIMPLEMENTED 
        
    assert f"Implementation missing" in err.details()

    help_info = get_rich_error(err, error_details_pb2.Help)
    
    assert help_info is not None, f"Method {method_name} missing 'Help' rich error details"
    assert help_info.links[0].description == "Check Documentation"
    assert "github.com" in help_info.links[0].url


@pytest.mark.parametrize(
    "method_name, impl_name",
    [
        ("boot", "_boot_impl"),
        ("restart", "_restart_impl"),
        ("kill", "_kill_impl"),
        ("terminate", "_terminate_impl"),
        ("ps", "_ps_impl"),
        ("logs", "_logs_impl"),
    ]
)
def test_all_methods_unhandled_exception(
    process_manager_rich_error_test_suite, 
    method_name, 
    impl_name, 
    boot_request
):
    """
    Parametrized test to verify that all ProcessManager methods correctly 
    handle DruncCommandExceptions by returning an INTERNAL error with ErrorInfo.
    """
    
    # Setup the test suite
    process_manager_rich_error_test_suite.setup_server_and_client()
    
    # Mock the specific implementation method
    exception_msg = f"Unexpected error in {method_name}"
    mock_impl = MagicMock(side_effect=ValueError(exception_msg))
    setattr(process_manager_rich_error_test_suite.servicer, impl_name, mock_impl)

    # Call the method via the stub
    stub_method = getattr(process_manager_rich_error_test_suite.stub, method_name)
    
    with pytest.raises(grpc.RpcError) as exc_info:
        stub_method(boot_request)
    
    err = exc_info.value
    assert err.code() == grpc.StatusCode.INTERNAL
    
    assert f"Unhandled exception in ProcessManager.{method_name}" in err.details()
    assert exception_msg in err.details()

    # 5. Assert Rich Error Details (ErrorInfo)
    error_info = get_rich_error(err, error_details_pb2.ErrorInfo)
    
    assert error_info is not None, f"Method {method_name} missing 'ErrorInfo' details"
    
    # Validate the structured metadata
    # (Assuming your code sets domain="ProcessManager.<method_name>")
    assert error_info.domain == f"ProcessManager.{method_name}"
    assert error_info.reason == "UNHANDLED_EXCEPTION"
    
    # Validate that the original python error was captured in metadata
    assert error_info.metadata["original_error"] == str(exception_msg)