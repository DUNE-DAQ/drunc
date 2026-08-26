"""Test Controller rich error handling with a real gRPC server with RichErrorServerInterceptor
and a real client stub with RichErrorClientInterceptor.

"""

from concurrent import futures
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import grpc
import pytest
from druncschema.controller_pb2 import (
    StatusRequest,
    StatusResponse,
)
from druncschema.controller_pb2_grpc import (
    ControllerServicer,
    ControllerStub,
    add_ControllerServicer_to_server,
)
from druncschema.token_pb2 import Token
from google.rpc import error_details_pb2, status_pb2

from drunc.controller.controller import Controller
from drunc.connectivity_service.exceptions import (
    ApplicationLookupUnsuccessful,
    ConnectivityServiceUnavailable,
)
from drunc.exceptions import DruncSetupException
from drunc.utils.grpc_utils import (
    RichErrorClientInterceptor,
    RichErrorServerInterceptor,
    ServerUnreachable,
    extract_grpc_rich_error,
)


class DummyControllerServicer(ControllerServicer):
    """
    Dummy ControllerServicer used to trigger `DruncException`s without
    building a full `Controller`. This could be replaced by any other
    Exception raised in the `Controller` methods.
    """

    def status(self, request: StatusRequest, context) -> StatusResponse:
        raise DruncSetupException(
            message="Controller is not ready",
            details="Controller has not finished initialising",
        )


class ControllerRichErrorTestSuite:
    """Test suite for rich error message propagation for the Controller."""

    def __init__(self):
        self.server_port = None
        self.server_address = None
        self.server = None
        self.channel = None
        self.stub = None
        self.servicer = None
        self.mock_client_logger = None

    def setup_server_and_client(self, servicer=None):
        """Initialise a real gRPC server and client for rich error handling."""
        self.servicer = servicer or DummyControllerServicer()

        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            interceptors=[RichErrorServerInterceptor()],
        )
        add_ControllerServicer_to_server(self.servicer, self.server)
        self.server_port = str(self.server.add_insecure_port("[::]:0"))
        self.server_address = f"localhost:{self.server_port}"
        self.server.start()

        # Create a mock logger for the client interceptor
        self.mock_client_logger = MagicMock()
        client_interceptor = RichErrorClientInterceptor(logger=self.mock_client_logger)

        raw_channel = grpc.insecure_channel(self.server_address)
        self.channel = grpc.intercept_channel(raw_channel, client_interceptor)
        self.stub = ControllerStub(self.channel)

    def teardown_server_and_client(self):
        """Clean up gRPC server and client resources."""
        if self.channel:
            self.channel.close()
        if self.server:
            self.server.stop(grace=0)
        self.stub = None
        self.servicer = None


@pytest.fixture(scope="function")
def controller_rich_error_test_suite():
    """
    Pytest fixture for ControllerRichErrorTestSuite.
    """
    suite = ControllerRichErrorTestSuite()
    yield suite
    suite.teardown_server_and_client()


@pytest.fixture(scope="function")
def status_request():
    return StatusRequest(
        token=Token(),
        target="",
        execute_along_path=False,
        execute_on_all_subsequent_children_in_path=True,
    )


def test_drunc_exception_rich_error(controller_rich_error_test_suite, status_request):
    """
    Test that a `DruncSetupException` raised by the server is returned to the
    client as a rich gRPC error, and that the client interceptor catches it and
    logs it.
    """
    controller_rich_error_test_suite.setup_server_and_client()
    stub = controller_rich_error_test_suite.stub

    # Patch but allow the real extract_grpc_rich_error to be called as this only happens
    # when the ClientInterceptor catches the RpcError
    with patch(
        "drunc.utils.grpc_utils.extract_grpc_rich_error",
        wraps=extract_grpc_rich_error,
    ) as mock_extract_grpc_rich_error:
        with pytest.raises(grpc.RpcError) as excinfo:
            stub.status(status_request)

    err = excinfo.value

    assert err.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "Controller is not ready" in err.details()

    # Unpack rich error metadata
    status = status_pb2.Status()
    for key, value in err.trailing_metadata():
        if key == "grpc-status-details-bin":
            status.ParseFromString(value)

            base_error = None
            precond = None

            for detail in status.details:
                if detail.Is(error_details_pb2.ErrorInfo.DESCRIPTOR):
                    base_error = error_details_pb2.ErrorInfo()
                    detail.Unpack(base_error)
                elif detail.Is(error_details_pb2.PreconditionFailure.DESCRIPTOR):
                    precond = error_details_pb2.PreconditionFailure()
                    detail.Unpack(precond)

            assert base_error is not None
            assert base_error.reason == "DruncSetupException"
            assert base_error.domain == "drunc"

            assert precond is not None
            assert len(precond.violations) > 0
            violation = precond.violations[0]
            assert violation.type == "MISSING OR INVALID"
            assert "Controller has not finished initialising" in violation.description

    mock_extract_grpc_rich_error.assert_called_once()
    controller_rich_error_test_suite.mock_client_logger.error.assert_called_once()


def make_localhost_connectivity_configuration():
    """
    Minimal controller configuration whose connectivity service points at localhost.

    """
    database = MagicMock()
    database.get_dal.return_value = SimpleNamespace(
        segment=SimpleNamespace(
            controller=SimpleNamespace(id="parent-controller"),
        )
    )

    return SimpleNamespace(
        db=database,
        oks_key=SimpleNamespace(session="test-session"),
        initial_data="",
        controller=SimpleNamespace(fsm={}),
        authoriser=SimpleNamespace(),
        session=SimpleNamespace(
            connectivity_service=SimpleNamespace(host="localhost"),
        ),
    )

class CallbackControllerServicer(ControllerServicer):
    """Run test scenario through the controller servicer."""

    def __init__(self, scenario):
        self.scenario = scenario

    def status(self, request: StatusRequest, context) -> StatusResponse:
        return self.scenario(request, context)


def scenario(request, context):
    """
    Test Controller construction in different scenarios. Could be used to force 
    missing configuration or dependencies failing.
    """
    
    with (
        patch("drunc.controller.controller.LogHandlerConf"),
        patch("drunc.controller.controller.setup_daq_ers_logger"),
        patch("drunc.controller.controller.FSMConfHandler.from_pyobject"),
        patch("drunc.controller.controller.StatefulNode"),
        patch("drunc.controller.controller.DummyAuthoriserConfHandler.from_pyobject"),
        patch("drunc.controller.controller.DummyAuthoriser"),
        patch("drunc.controller.controller.ControllerActor"),
        patch.dict("os.environ", {"CONNECTION_PORT": "5000"}, clear=True),
    ):
        Controller(
            configuration=make_localhost_connectivity_configuration(),
            name="parent-controller",
            session="test-session",
            token=Token(),
        )

    raise AssertionError("Controller construction should have raised")


def test_init_controller_lookup_failure(
    controller_rich_error_test_suite,
    status_request,
):
    """
    Test init Controller with a failing connectivity service lookup.
    """
    stateful_node = MagicMock()

    def scenario(request, context):
        controller = Controller.__new__(Controller)
        controller.name = "parent-controller"
        controller.session = "test-session"
        controller.log = MagicMock()
        controller.actor = MagicMock()
        controller.actor.get_token.return_value = Token()
        controller.connectivity_service = None
        controller.connectivity_service_thread = None
        controller.stateful_node = stateful_node
        controller.children_nodes = []
        controller.opmon_publisher = None
        controller.running = False

        controller.configuration = SimpleNamespace(
            init_children=MagicMock(
                side_effect=ApplicationLookupUnsuccessful(
                    message="The original connectivity lookup failed"
                )
            )
        )

        controller.init_controller()
        raise AssertionError("init_controller() should have raised")

    controller_rich_error_test_suite.setup_server_and_client(
        CallbackControllerServicer(scenario)
    )

    with pytest.raises(grpc.RpcError) as excinfo:
        controller_rich_error_test_suite.stub.status(status_request)

    error = excinfo.value
    expected_message = (
        "Failed to find all child applications on the connectivity service. "
        "Check that all children are up and registered to the connectivity service."
    )

    assert error.code() == grpc.StatusCode.NOT_FOUND
    assert error.details() == expected_message
    stateful_node.to_error.assert_called_once_with()


def test_missing_drunc_host_name_from_controller(
    controller_rich_error_test_suite,
    status_request,
):
    """Test init Controller with missing DRUNC_HOST_NAME."""

    def scenario(request, context):
        with (
            patch("drunc.controller.controller.LogHandlerConf"),
            patch("drunc.controller.controller.setup_daq_ers_logger"),
            patch("drunc.controller.controller.FSMConfHandler.from_pyobject"),
            patch("drunc.controller.controller.StatefulNode"),
            patch(
                "drunc.controller.controller.DummyAuthoriserConfHandler.from_pyobject"
            ),
            patch("drunc.controller.controller.DummyAuthoriser"),
            patch("drunc.controller.controller.ControllerActor"),
            patch.dict("os.environ", {"CONNECTION_PORT": "5000"}, clear=True),
        ):
            Controller(
                configuration=make_localhost_connectivity_configuration(),
                name="parent-controller",
                session="test-session",
                token=Token(),
            )

        raise AssertionError("Controller construction should have raised")

    controller_rich_error_test_suite.setup_server_and_client(
        CallbackControllerServicer(scenario)
    )

    with pytest.raises(grpc.RpcError) as excinfo:
        controller_rich_error_test_suite.stub.status(status_request)

    error = excinfo.value

    assert error.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "DRUNC_HOST_NAME environment variable is not set." in error.details()

    rich_error = extract_grpc_rich_error(error)
    assert rich_error.code == "FAILED_PRECONDITION"
    assert rich_error.message == "DRUNC_HOST_NAME environment variable is not set."

    preconditions = [
        detail
        for detail in rich_error.details
        if isinstance(detail, error_details_pb2.PreconditionFailure)
    ]
    assert len(preconditions) == 1

    violation = preconditions[0].violations[0]
    assert violation.type == "MISSING OR INVALID"
    assert violation.description == (
        "Controller cannot connect to the connectivity service because "
        "the DRUNC_HOST_NAME environment variable is not set."
    )

def test_advertise_control_address_unavailable(
    controller_rich_error_test_suite,
    status_request,
):
    """Test that an unavailable connectivity service is returned as a rich gRPC error."""

    connectivity_service = MagicMock()
    connectivity_service.address = "connectivity-service:5000"
    connectivity_service.is_ready.return_value = False

    controller = Controller.__new__(Controller)
    controller.name = "parent-controller"
    controller.log = MagicMock()
    controller.uri = ""
    controller.running = False
    controller.connectivity_service = connectivity_service
    controller.connectivity_service_thread = None
    controller.children_nodes = []
    controller.opmon_publisher = None
    controller.stop_event = None

    def scenario(request, context):
        controller.advertise_control_address("grpc://parent-controller:5001")
        raise AssertionError("advertise_control_address() should have raised")

    controller_rich_error_test_suite.setup_server_and_client(
        CallbackControllerServicer(scenario)
    )

    # as it returns UNAVAILABLE, the client interceptor should raise ServerUnreachable
    with pytest.raises(ServerUnreachable) as excinfo:
        controller_rich_error_test_suite.stub.status(status_request)

    error = excinfo.value
    expected_message = (
        "Connectivity service unavailable for control address advertising."
    )

    assert str(error) == expected_message

    grpc_error = error.__cause__
    assert isinstance(grpc_error, grpc.RpcError)
    assert grpc_error.code() == grpc.StatusCode.UNAVAILABLE
    assert grpc_error.details() == expected_message

    rich_error = extract_grpc_rich_error(grpc_error)
    assert rich_error.code == "UNAVAILABLE"
    assert rich_error.message == expected_message