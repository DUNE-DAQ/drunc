import time

import grpc
from druncschema.controller_pb2 import AddressedCommand
from druncschema.controller_pb2_grpc import ControllerStub
from druncschema.request_response_pb2 import Request, Response

from drunc.broadcast.client.broadcast_handler import BroadcastHandler
from drunc.broadcast.client.configuration import BroadcastClientConfHandler
from drunc.controller.children_interface.child_node import ChildNode
from drunc.controller.utils import handle_controller_grpc_error
from drunc.exceptions import DruncSetupException
from drunc.utils.configuration import ConfHandler, ConfTypes
from drunc.utils.grpc_utils import ServerUnreachable, copy_token
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

        n_tries = 20
        while True:
            n_tries -= 1

            try:
                response = self.stub.describe(request)

            except grpc.RpcError as error:
                try:
                    handle_controller_grpc_error(error)
                except ServerUnreachable as server_unreachable_error:
                    if n_tries == 0:
                        raise server_unreachable_error
                    self.log.info(
                        (
                            f"Could not connect to the controller ({self.uri}). "
                            f"Trying {n_tries} more times..."
                        )
                    )
                    time.sleep(5)

            else:
                self.log.info(f"Connected to the controller ({self.uri})!")
                self.start_listening(response.broadcast)
                break

    def __str__(self):
        return f"'{self.name}@{self.uri}' (type {self.node_type})"

    def get_endpoint(self):
        return self.uri

    def start_listening(self, bdesc):
        self.broadcast = BroadcastHandler(
            BroadcastClientConfHandler(
                data=bdesc,
                type=ConfTypes.ProtobufAny,
            )
        )

    def send_command(self, token, command: str, data=None):
        log = get_logger("controller.send_command")

        # Grab the command from the controller stub in the context
        # Add the token to the data (which can be of any protobuf type)
        # Send the command to the controller

        cmd = getattr(self.stub, command)  # this throws if the command doesn't exist

        request = Request(token=token)
        if data is not None:
            request.data.Pack(data)

        log.debug(f"Sending: {command} to the controller, with {request=}")

        try:
            response = cmd(request)
        except grpc.RpcError as e:
            handle_controller_grpc_error(e)

        return response

    def terminate(self):
        if self.channel:
            self.channel.close()
            del self.channel
        if self.stub:
            del self.stub

        self.channel = None
        self.stub = None
        self.broadcast.stop()

    def propagate_command(self, command, data, token) -> Response:
        return self.send_command(
            token=token,
            command=command,
            data=data,
        )
