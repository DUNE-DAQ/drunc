import threading
import time
from typing import NoReturn, cast

import grpc
from druncschema.common_pb2 import LogOnServerRequest, LogOnServerResponse
from druncschema.controller_pb2 import (
    DescribeFSMRequest,
    DescribeFSMResponse,
    DescribeRequest,
    DescribeResponse,
    ExcludeRequest,
    ExcludeResponse,
    ExecuteExpertCommandRequest,
    ExecuteExpertCommandResponse,
    ExecuteFSMCommandRequest,
    ExecuteFSMCommandResponse,
    FSMCommand,
    IncludeRequest,
    IncludeResponse,
    RecomputeStatusRequest,
    RecomputeStatusResponse,
    StatusRequest,
    StatusResponse,
    SurrenderControlRequest,
    SurrenderControlResponse,
    TakeControlRequest,
    TakeControlResponse,
    ToErrorRequest,
    ToErrorResponse,
    WhoIsInChargeRequest,
    WhoIsInChargeResponse,
)
from druncschema.controller_pb2_grpc import ControllerStub
from druncschema.generic_pb2 import PlainText, Stacktrace
from druncschema.token_pb2 import Token
from grpc_status import rpc_status

from drunc.connectivity_service.exceptions import (
    ApplicationLookupUnsuccessful,
)
from drunc.controller.children_interface.child_node import ChildNode
from drunc.exceptions import DruncSetupException
from drunc.grpc_settings import CONTROLLER_CLIENT_GRPC_CONFIG
from drunc.utils.configuration import ConfHandler
from drunc.utils.grpc_utils import (
    ServerUnreachable,
    rethrow_if_unreachable_server,
    unpack_any,
)
from drunc.utils.utils import (
    ControlType,
    get_control_type_and_uri_from_connectivity_service,
)


class gRCPChildConfHandler(ConfHandler):
    def get_uri(self):
        for service in self.data.controller.exposes_service:
            if self.data.controller.id + "_control" in service.id:
                return f"{service.protocol}://{self.data.controller.runs_on.runs_on.id}:{service.port}"
        raise DruncSetupException(
            f"gRPC API child node {self.data.controller.id} does not expose a control service"
        )


class gRPCChildNode(ChildNode):
    def __init__(
        self,
        name: str,
        configuration: gRCPChildConfHandler,
        uri: str,
        connectivity_service,
        init_token: Token | None = None,
    ):
        super().__init__(name, ControlType.gRPC)

        self.configuration = configuration
        self.connectivity_service = connectivity_service
        self._lock = threading.Lock()
        self.init_token = init_token

        host, port = uri.split(":")
        port = int(port)

        if port == 0:
            raise DruncSetupException(
                f"Application {name} does not expose a control service in the configuration, or has not advertised itself to the application registry service, or the application registry service is not reachable."
            )

        self.uri = f"{host}:{port}"
        self._setup_connection()

    def _setup_connection(self):
        """Setup the gRPC connection to the child controller"""
        with self._lock:
            if hasattr(self, "channel") and self.channel:
                self.channel.close()

            self.channel = grpc.insecure_channel(
                self.uri, options=CONTROLLER_CLIENT_GRPC_CONFIG
            )
            self.log.info(f"Created new gRPC channel to {self.uri}")
            self.stub = ControllerStub(self.channel)

        request = DescribeRequest(
            token=None,
            target="",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        )

        tries_remaining = 20
        while True:
            tries_remaining -= 1

            try:
                self.stub.describe(request)

            except grpc.RpcError as error:
                if tries_remaining == 0:
                    raise error
                self.log.info(
                    (
                        f"Could not connect to the controller ({self.uri}). "
                        f"Trying {tries_remaining} more times..."
                    )
                )
                time.sleep(5)

            else:
                self.log.info(f"Connected to the controller ({self.uri})!")
                break

    def _attempt_reconnection(self, retry_call):
        """Handle ServerUnreachable errors with automatic reconnection and retry.

        Args:
            retry_call: A callable that retries the original operation
        """
        if not self.connectivity_service:
            self.log.error(f"No connectivity service available for {self.name}")
            raise ServerUnreachable("No connectivity service available")

        try:
            ctype, new_uri = get_control_type_and_uri_from_connectivity_service(
                self.connectivity_service, self.name, timeout=10
            )

            if new_uri != self.uri:
                self.log.info(
                    f"Found new IP {new_uri} for {self.name}, reconnecting..."
                )
                self.uri = new_uri
                self._setup_connection()
            else:
                self.log.info(f"Reconnecting to same address {self.uri}...")
                self._setup_connection()

            # Retry the original call
            return retry_call()

        except ApplicationLookupUnsuccessful:
            self.log.error(f"Child {self.name} not found in connectivity service")
            raise ServerUnreachable(
                f"Child {self.name} not found in connectivity service"
            )
        except Exception as reconnect_error:
            self.log.error(f"Failed to reconnect to {self.name}: {reconnect_error}")
            raise ServerUnreachable(
                f"Failed to reconnect: {reconnect_error}"
            ) from reconnect_error

    def __str__(self) -> str:
        return f"'{self.name}@{self.uri}' (type {self.node_type})"

    def get_endpoint(self) -> str:
        return self.uri

    def terminate(self) -> None:
        if self.channel:
            self.channel.close()
            del self.channel
        if self.stub:
            del self.stub
        self.channel = None

    def check_connection(self) -> bool:
        """Probe child connectivity and retry once after reconnecting if needed.

        Use the describe endpoint to check if the child is reachable. If not,
        the node attempts to resolve a fresh endpoint from the connectivity
        service, rebuild the gRPC channel, and retry the probe once.

        Returns:
            True if the child is reachable either immediately or after a successful
            reconnection attempt, otherwise False.
        """
        request = DescribeRequest(
            token=None,
            target="",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        )

        try:
            self.stub.describe(request)
            return True
        except grpc.RpcError as error:
            try:
                self.handle_child_grpc_error(error)
            except ServerUnreachable:
                self.log.info(
                    f"Connection to {self.name} at {self.uri} failed during connectivity check, attempting to reconnect..."
                )
                try:
                    self._attempt_reconnection(lambda: self.stub.describe(request))
                    return True
                except Exception as reconnect_error:
                    self.log.warning(
                        f"Connection check failed for {self.name}: {reconnect_error}"
                    )
                    return False
            except Exception as unexpected_error:
                self.log.warning(
                    f"Connection check failed for {self.name}: {unexpected_error}"
                )
                return False

        return False

    def status(
        self,
        target: str = "",
        execute_along_path: bool = False,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> StatusResponse:
        request = StatusRequest(
            token=None,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        try:
            response = self.stub.status(request)
        except grpc.RpcError as error:
            try:
                self.handle_child_grpc_error(error)
            except ServerUnreachable:
                self.log.info(
                    f"Connection to {self.name} at {self.uri} failed during status check, attempting to reconnect..."
                )
                response = self._attempt_reconnection(lambda: self.stub.status(request))

        return response

    def describe(
        self,
        target: str = "",
        execute_along_path: bool = False,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> DescribeResponse:
        request = DescribeRequest(
            token=None,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        try:
            response = self.stub.describe(request)
        except grpc.RpcError as error:
            try:
                self.handle_child_grpc_error(error)
            except ServerUnreachable:
                self.log.info(
                    f"Connection to {self.name} at {self.uri} failed during describe check, attempting to reconnect..."
                )
                response = self._attempt_reconnection(
                    lambda: self.stub.describe(request)
                )

        return response

    def describe_fsm(
        self,
        target: str = "",
        execute_along_path: bool = False,
        execute_on_all_subsequent_children_in_path: bool = True,
        key: str = "",
    ) -> DescribeFSMResponse:
        request = DescribeFSMRequest(
            token=None,
            key=key,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        try:
            response = self.stub.describe_fsm(request)
        except grpc.RpcError as error:
            try:
                self.handle_child_grpc_error(error)
            except ServerUnreachable:
                self.log.info(
                    f"Connection to {self.name} at {self.uri} failed during describe_fsm check, attempting to reconnect..."
                )
                response = self._attempt_reconnection(
                    lambda: self.stub.describe_fsm(request)
                )

        return response

    def execute_fsm_command(
        self,
        command: FSMCommand,
        target: str = "",
        execute_along_path: bool = False,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> ExecuteFSMCommandResponse:
        request = ExecuteFSMCommandRequest(
            token=None,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )
        request.command.CopyFrom(command)

        try:
            response = self.stub.execute_fsm_command(request)
        except grpc.RpcError as error:
            try:
                self.handle_child_grpc_error(error)
            except ServerUnreachable:
                self.log.info(
                    f"Connection to {self.name} at {self.uri} failed, attempting to reconnect..."
                )
                response = self._attempt_reconnection(
                    lambda: self.stub.execute_fsm_command(request)
                )

        return response

    def execute_expert_command(
        self,
        json_string: str,
        target: str = "",
        execute_along_path: bool = False,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> ExecuteExpertCommandResponse:
        request = ExecuteExpertCommandRequest(
            token=None,
            json_string=json_string,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        try:
            response = self.stub.execute_expert_command(request)
        except grpc.RpcError as error:
            try:
                self.handle_child_grpc_error(error)
            except ServerUnreachable:
                self.log.info(
                    f"Connection to {self.name} at {self.uri} failed, attempting to reconnect..."
                )
                response = self._attempt_reconnection(
                    lambda: self.stub.execute_expert_command(request)
                )

        return response

    def include(
        self,
        target: str = "",
        execute_along_path: bool = False,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> IncludeResponse:
        request = IncludeRequest(
            token=None,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )
        self.included = True

        try:
            response = self.stub.include(request)
        except grpc.RpcError as e:
            try:
                self.handle_child_grpc_error(e)
            except ServerUnreachable:
                self.log.warning(
                    f"Connection to {self.name} at {self.uri} failed during include, attempting to reconnect..."
                )
                response = self._attempt_reconnection(
                    lambda: self.stub.include(request)
                )

        return response

    def exclude(
        self,
        target: str = "",
        execute_along_path: bool = False,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> ExcludeResponse:
        request = ExcludeRequest(
            token=None,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )
        self.included = False

        try:
            response = self.stub.exclude(request)
        except grpc.RpcError as e:
            try:
                self.handle_child_grpc_error(e)
            except ServerUnreachable:
                self.log.warning(
                    f"Connection to {self.name} at {self.uri} failed during exclude, attempting to reconnect..."
                )
                response = self._attempt_reconnection(
                    lambda: self.stub.exclude(request)
                )

        return response

    def recompute_status(
        self,
        target: str = "",
        execute_along_path: bool = False,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> RecomputeStatusResponse:
        request = RecomputeStatusRequest(
            token=None,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        try:
            response = self.stub.recompute_status(request)
        except grpc.RpcError as e:
            try:
                self.handle_child_grpc_error(e)
            except ServerUnreachable:
                self.log.info(
                    f"Connection to {self.name} at {self.uri} failed during recompute_status, attempting to reconnect..."
                )
                response = self._attempt_reconnection(
                    lambda: self.stub.recompute_status(request)
                )

        return response

    def take_control(
        self,
        target: str = "",
        execute_along_path: bool = False,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> TakeControlResponse:
        request = TakeControlRequest(
            token=None,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        try:
            response = self.stub.take_control(request)
        except grpc.RpcError as e:
            try:
                self.handle_child_grpc_error(e)
            except ServerUnreachable:
                self.log.info(
                    f"Connection to {self.name} at {self.uri} failed during take_control, attempting to reconnect..."
                )
                response = self._attempt_reconnection(
                    lambda: self.stub.take_control(request)
                )

        return response

    def surrender_control(
        self,
        target: str = "",
        execute_along_path: bool = False,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> SurrenderControlResponse:
        request = SurrenderControlRequest(
            token=None,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        try:
            response = self.stub.surrender_control(request)
        except grpc.RpcError as e:
            try:
                self.handle_child_grpc_error(e)
            except ServerUnreachable:
                self.log.info(
                    f"Connection to {self.name} at {self.uri} failed during surrender_control, attempting to reconnect..."
                )
                response = self._attempt_reconnection(
                    lambda: self.stub.surrender_control(request)
                )

        return response

    def who_is_in_charge(
        self,
        target: str = "",
        execute_along_path: bool = False,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> WhoIsInChargeResponse:
        request = WhoIsInChargeRequest(
            token=None,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        try:
            response = self.stub.who_is_in_charge(request)
        except grpc.RpcError as e:
            try:
                self.handle_child_grpc_error(e)
            except ServerUnreachable:
                self.log.info(
                    f"Connection to {self.name} at {self.uri} failed during who_is_in_charge, attempting to reconnect..."
                )
                response = self._attempt_reconnection(
                    lambda: self.stub.who_is_in_charge(request)
                )

        return response

    def to_error(
        self,
        target: str = "",
        execute_along_path: bool = False,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> ToErrorResponse:
        request = ToErrorRequest(
            token=None,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        try:
            response = self.stub.to_error(request)
        except grpc.RpcError as e:
            try:
                self.handle_child_grpc_error(e)
            except ServerUnreachable:
                self.log.info(
                    f"Connection to {self.name} at {self.uri} failed during to_error, attempting to reconnect..."
                )
                response = self._attempt_reconnection(
                    lambda: self.stub.to_error(request)
                )

        return response

    def handle_child_grpc_error(self, error: grpc.RpcError) -> NoReturn:
        """Handle gRPC errors from sending commands to the child controller.

        Args:
            error: The gRPC error to handle.
        """
        rethrow_if_unreachable_server(error)

        # RpcError is also a subclass of Call, and can be used in from_call.
        # The type stubs in types-grpcio do not reflect this, so we must cast.
        # See https://github.com/grpc/grpc/issues/10885.
        status = rpc_status.from_call(cast(grpc.Call, error))

        self.log.error(f"Error sending command to child node {self.name} at {self.uri}")

        if hasattr(status, "message"):
            self.log.error(status.message)

        if hasattr(status, "details"):
            for detail in status.details:
                if detail.Is(Stacktrace.DESCRIPTOR):
                    text = "Stacktrace on remote server!\n"
                    stack = unpack_any(detail, Stacktrace)
                    for l in stack.text:
                        text += l + "\n"
                    self.log.error(text)
                elif detail.Is(PlainText.DESCRIPTOR):
                    text = unpack_any(detail, PlainText)
                    self.log.error(text)

        raise error

    def log_on_server(
        self,
        text: str,
        severity: str = "INFO",
        target: str = "",
        execute_along_path: bool = False,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> LogOnServerResponse:
        request = LogOnServerRequest(
            token=None,
            text=text,
            severity=severity,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        try:
            response = self.stub.log_on_server(request)
        except grpc.RpcError as e:
            try:
                self.handle_child_grpc_error(e)
            except ServerUnreachable:
                self.log.info(
                    f"Connection to {self.name} at {self.uri} failed during who_is_in_charge, attempting to reconnect..."
                )
                response = self._attempt_reconnection(
                    lambda: self.stub.who_is_in_charge(request)
                )

        return response
