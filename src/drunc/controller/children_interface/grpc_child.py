import threading
import time

import grpc
from druncschema.controller_pb2_grpc import ControllerStub
from druncschema.description_pb2 import OldDescription
from druncschema.request_response_pb2 import Response

from drunc.broadcast.client.broadcast_handler import BroadcastHandler
from drunc.broadcast.client.configuration import BroadcastClientConfHandler
from drunc.controller.children_interface.child_node import ChildNode
from drunc.controller.utils import send_command
from drunc.exceptions import DruncSetupException
from drunc.utils.configuration import ConfHandler, ConfTypes
from drunc.utils.grpc_utils import ServerUnreachable
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
            name=name, node_type=ControlType.gRPC, configuration=configuration
        )

        self.log = get_logger(f"controller.{self.name}-grpc-child")
        self.connectivity_service = connectivity_service
        self.init_token = init_token
        self._lock = threading.Lock()

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

        desc = OldDescription()
        ntries = 20

        for itry in range(ntries):
            try:
                response = send_command(
                    controller=self.controller,
                    token=self.init_token,
                    command="describe",
                    rethrow=True,
                )
                response.data.Unpack(desc)
            except ServerUnreachable as e:
                if itry + 1 == ntries:
                    raise e
                else:
                    self.log.info(
                        f"Could not connect to the controller ({self.uri}), trial {itry + 1} of {ntries}"
                    )
                    time.sleep(5)
                    continue

            else:
                self.log.info(f"Connected to the controller ({self.uri})!")
                break

        self.start_listening(desc.broadcast)

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

    def terminate(self):
        if self.channel:
            self.channel.close()
            del self.channel
        if self.controller:
            del self.controller

        self.controller = None
        self.channel = None
        self.broadcast.stop()

    def propagate_command(self, command, data, token) -> Response:
        try:
            return send_command(
                controller=self.controller,
                token=token,
                command=command,
                rethrow=True,
                data=data,
            )
        except ServerUnreachable as e:
            self.log.warning(
                f"Connection to {self.name} failed, attempting to reconnect..."
            )
            try:
                # Try to reconnect using connectivity service
                if self.connectivity_service:
                    from drunc.connectivity_service.exceptions import (
                        ApplicationLookupUnsuccessful,
                    )
                    from drunc.utils.utils import (
                        get_control_type_and_uri_from_connectivity_service,
                    )

                    try:
                        ctype, new_uri = (
                            get_control_type_and_uri_from_connectivity_service(
                                self.connectivity_service, self.name, timeout=10
                            )
                        )
                        if new_uri != self.uri:
                            self.log.info(
                                f"Found new IP {new_uri} for {self.name}, reconnecting..."
                            )
                            self.uri = new_uri
                            self._setup_connection()

                            # Retry the command with new connection
                            return send_command(
                                controller=self.controller,
                                token=token,
                                command=command,
                                rethrow=True,
                                data=data,
                            )
                    except ApplicationLookupUnsuccessful:
                        self.log.error(
                            f"Child {self.name} not found in connectivity service"
                        )
                        raise e

            except Exception as reconnect_error:
                self.log.error(f"Failed to reconnect to {self.name}: {reconnect_error}")
                raise e
