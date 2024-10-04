from drunc.controller.children_interface.child_node import ChildNode
from drunc.utils.utils import ControlType
from drunc.controller.utils import send_command
from drunc.utils.configuration import ConfHandler
import grpc as grpc
from drunc.exceptions import DruncSetupException
from druncschema.request_response_pb2 import Response


class gRCPChildConfHandler(ConfHandler):
    def get_uri(self):
        for service in self.data.controller.exposes_service:
            if self.data.controller.id+"_control" in service.id:
                return f"{service.protocol}://{self.data.controller.runs_on.runs_on.id}:{service.port}"
        raise DruncSetupException(f"gRPC API child node {self.data.controller.id} does not expose a control service")


class gRPCChildNode(ChildNode):
    def __init__(self, name, configuration:gRCPChildConfHandler, init_token, uri):
        super().__init__(
            name = name,
            node_type = ControlType.gRPC
        )

        from logging import getLogger
        self.log = getLogger(f'{self.name}-grpc-child')
        self.configuration = configuration

        host, port = uri.split(":")
        port = int(port)

        if port == 0:
            from drunc.exceptions import DruncSetupException
            raise DruncSetupException(f"Application {name} does not expose a control service in the configuration, or has not advertised itself to the application registry service, or the application registry service is not reachable.")

        self.uri = f"{host}:{port}"

        from druncschema.controller_pb2_grpc import ControllerStub
        import grpc

        self.channel = grpc.insecure_channel(self.uri)
        self.controller = ControllerStub(self.channel)

        from druncschema.request_response_pb2 import Description
        desc = Description()
        ntries = 20
        from drunc.utils.grpc_utils import ServerUnreachable
        from drunc.exceptions import DruncSetupException

        for itry in range(ntries):
            try:
                response = send_command(
                    controller = self.controller,
                    token = init_token,
                    command = 'describe',
                    rethrow = True
                )
                response.data.Unpack(desc)
            except ServerUnreachable as e:
                if itry+1 == ntries:
                    raise e
                else:
                    self.log.info(f'Could not connect to the controller ({self.uri}), trial {itry+1} of {ntries}')
                    from time import sleep
                    sleep(5)

            except ServerUnreachable as e:
                raise DruncSetupException from e
            else:
                self.log.info(f'Connected to the controller ({self.uri})!')
                break
        self.start_listening(desc.broadcast)

    def __str__(self):
        return f'\'{self.name}@{self.uri}\' (type {self.node_type})'

    def get_endpoint(self):
        return self.uri


    def start_listening(self, bdesc):
        from drunc.broadcast.client.configuration import BroadcastClientConfHandler
        from drunc.broadcast.client.broadcast_handler import BroadcastHandler
        from drunc.utils.configuration import ConfTypes
        self.broadcast = BroadcastHandler(
            BroadcastClientConfHandler(
                data = bdesc,
                type = ConfTypes.ProtobufAny,
            )
        )

    def get_status(self, token) -> Response:
        from druncschema.controller_pb2 import Status
        from drunc.utils.grpc_utils import unpack_any

        status = unpack_any(
            send_command(
                controller = self.controller,
                token = token,
                command = 'get_status',
                data = None
            ).data,
            Status
        )

        return status

    def terminate(self):
        if self.channel:
            self.channel.close()
            del self.channel
        if self.controller:
            del self.controller

        self.controller = None
        self.channel = None
        self.broadcast.stop()
        pass

    def propagate_command(self, command, data, token) -> Response:

        return send_command(
            controller = self.controller,
            token = token,
            command = command,
            rethrow = True,
            data = data
        )


