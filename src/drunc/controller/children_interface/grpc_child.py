import time
from typing import NoReturn, cast

import grpc
from druncschema.controller_pb2 import (
    AddressedCommand,
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
    def __init__(self, name, configuration: gRCPChildConfHandler, init_token, uri):
        super().__init__(
            name=name, node_type=ControlType.gRPC, configuration=configuration
        )

        self.log = get_logger(f"controller.{self.name}-grpc-child")

        host, port = uri.split(":")
        port = int(port)

        if port == 0:
            raise DruncSetupException(
                f"Application {name} does not expose a control service in the configuration, or has not advertised itself to the application registry service, or the application registry service is not reachable."
            )

        self.uri = f"{host}:{port}"

        self.channel = grpc.insecure_channel(self.uri)
        self.stub = ControllerStub(self.channel)

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
