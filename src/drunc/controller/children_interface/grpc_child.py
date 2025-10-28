import threading
import time
from typing import NoReturn, cast

import grpc
from druncschema.controller_pb2 import (
    AddressedCommand,
    DescribeFSMResponse,
    DescribeResponse,
    StatusResponse,
)
from druncschema.controller_pb2_grpc import ControllerStub
from druncschema.generic_pb2 import PlainText, Stacktrace
from druncschema.request_response_pb2 import Request, Response
from druncschema.token_pb2 import Token
from grpc_status import rpc_status

from drunc.broadcast.client.broadcast_handler import BroadcastHandler
from drunc.broadcast.client.configuration import BroadcastClientConfHandler
from drunc.controller.children_interface.child_node import ChildNode
from drunc.exceptions import DruncSetupException
from drunc.utils.configuration import ConfHandler, ConfTypes
from drunc.utils.grpc_utils import (
    ServerUnreachable,
    copy_token,
    rethrow_if_unreachable_server,
    unpack_any,
)
from drunc.utils.utils import ControlType, get_logger


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
        name,
        configuration: gRCPChildConfHandler,
        init_token,
        uri,
        connectivity_service=None,
    ):
        super().__init__(
            name=name,
            node_type=ControlType.gRPC,
            configuration=configuration,
        )

        self.log = get_logger(f"controller.{self.name}-grpc-child")
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

            self.channel = grpc.insecure_channel(self.uri)
            self.controller = ControllerStub(self.channel)

        request = AddressedCommand(
            token=copy_token(init_token),
            command_name="describe",
            command_data=None,
            target="",
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=False,
        )

        tries_remaining = 20
        while True:
            tries_remaining -= 1

            try:
                response = self.stub.describe(request)

            except grpc.RpcError as error:
                try:
                    self.handle_child_grpc_error(error)
                except ServerUnreachable as server_unreachable_error:
                    if tries_remaining == 0:
                        raise server_unreachable_error
                    self.log.info(
                        (
                            f"Could not connect to the controller ({self.uri}). "
                            f"Trying {tries_remaining} more times..."
                        )
                    )
                    time.sleep(5)

            else:
                self.log.info(f"Connected to the controller ({self.uri})!")
                self.start_listening(response.description.broadcast)
                break

    def _attempt_reconnection(self, token, command, data):
        """Attempt to reconnect using connectivity service and retry command"""
        if not self.connectivity_service:
            self.log.error(f"No connectivity service available for {self.name}")
            return None

        try:
            from drunc.connectivity_service.exceptions import (
                ApplicationLookupUnsuccessful,
            )
            from drunc.utils.utils import (
                get_control_type_and_uri_from_connectivity_service,
            )

            self.log.debug(f"Checking connectivity service for {self.name}...")
            ctype, new_uri = get_control_type_and_uri_from_connectivity_service(
                self.connectivity_service, self.name, timeout=10
            )

            if new_uri != self.uri:
                self.log.info(
                    f"Found new IP {new_uri} for {self.name}, reconnecting..."
                )
                self.uri = new_uri
                self._reconnect_to_new_uri()
            else:
                self.log.info(
                    f"IP address for {self.name} has not changed, reconnecting to same address..."
                )
                self._reconnect_to_new_uri()

            # Give control back to the child controller after reconnection
            self.log.info(f"Taking control of {self.name} after reconnection...")
            try:
                self.propagate_command("take_control", None, token)
                self.log.info(f"Successfully took control of {self.name}")
            except Exception as control_error:
                self.log.warning(
                    f"Failed to take control of {self.name}: {control_error}"
                )

            # Retry the original command
            self.log.info(f"Retrying original command {command} to {self.name}...")
            return self.propagate_command(command, data, token)

        except ApplicationLookupUnsuccessful:
            self.log.error(f"Child {self.name} not found in connectivity service")
            return None
        except Exception as reconnect_error:
            self.log.error(f"Failed to reconnect to {self.name}: {reconnect_error}")
            return None

    def _reconnect_to_new_uri(self):
        """Reconnect to the new URI for gRPC child"""
        self._setup_connection()

    def __str__(self):
        return f"'{self.name}@{self.uri}' (type {self.node_type})"

    def get_endpoint(self) -> str | None:
        return self.uri

    def start_listening(self, bdesc):
        self.broadcast = BroadcastHandler(
            BroadcastClientConfHandler(
                data=bdesc,
                type=ConfTypes.ProtobufAny,
            )
        )

    def terminate(self):
        if self.channel:
            self.channel.close()
            del self.channel
        if self.stub:
            del self.stub

        self.channel = None
        self.broadcast.stop()


    def propagate_command(
        self,
        command: str,
        request: AddressedCommand,
        token: Token | None,
    ) -> Response:
        packed_request = Request(token=token)
        packed_request.data.Pack(request)

        cmd = getattr(self.stub, command)

        try:
            response = cmd(packed_request)
        except grpc.RpcError as error:
            self.handle_child_grpc_error(error)
            
        except ServerUnreachable as e:
            self.log.warning(
                f"Connection to {self.name} failed, attempting to reconnect..."
            )
            result = self._attempt_reconnection(token, command, data)
            if result is not None:
                return result
            else:
                raise e

        return response

    def status(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> StatusResponse:
        request = AddressedCommand(
            token=None,
            command_name="status",
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        try:
            response = self.stub.status(request)
        except grpc.RpcError as error:
            self.handle_child_grpc_error(error)

        return response

    def describe(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> DescribeResponse:
        request = AddressedCommand(
            token=None,
            command_name="describe",
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        try:
            response = self.stub.describe(request)
        except grpc.RpcError as error:
            self.handle_child_grpc_error(error)

        return response

    def describe_fsm(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
        key: str = "",
    ) -> DescribeFSMResponse:
        request = AddressedCommand(
            token=None,
            command_name="describe_fsm",
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )
        request.command_data.Pack(PlainText(text=key))

        try:
            response = self.stub.describe_fsm(request)
        except grpc.RpcError as error:
            self.handle_child_grpc_error(error)

        return response

    def recompute_status(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
        timeout: int | float = 60,
    ) -> StatusResponse:
        request = AddressedCommand(
            token=None,
            command_name="recompute_status",
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        try:
            response = self.stub.recompute_status(request, timeout=timeout)
        except grpc.RpcError as e:
            self.handle_child_grpc_error(e)

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
