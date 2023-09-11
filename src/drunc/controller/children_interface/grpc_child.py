from drunc.controller.children_interface.child_node import ChildNode
from drunc.controller.utils import send_command
from drunc.broadcast.client.broadcast_handler import BroadcastHandler
import grpc as grpc

class gRPCChildNode(ChildNode):
    def __init__(self, name, child_conf, conf_type, **kwargs):
        super(gRPCChildNode, self).__init__(
            name = name,
            **kwargs
        )
        from logging import getLogger
        self.log = getLogger(f'{name}-grpc-child')

        from drunc.utils.conf_types import ConfTypes, ConfTypeNotSupported

        if conf_type == ConfTypes.Json:
            self.uri = child_conf['uri']

        else:
            raise ConfTypeNotSupported(conf_type, 'gRPCChildNode')

        from druncschema.controller_pb2_grpc import ControllerStub
        import grpc
        self.channel = grpc.insecure_channel(self.uri)
        self.controller = ControllerStub(self.channel)

        from druncschema.request_response_pb2 import Description
        desc = Description()
        ntries = 5
        from drunc.utils.grpc_utils import ServerUnreachable

        for itry in range(ntries):
            try:
                response = send_command(
                    controller = self.controller,
                    token = self.token,
                    command = 'describe',
                    rethrow = True
                )
                response.data.Unpack(desc)
            except ServerUnreachable as e:
                if itry+1 == ntries:
                    raise e
                else:
                    self.log.error(f'Could not connect to the controller, trial {itry+1} of {ntries}')
                    from time import sleep
                    sleep(0.5)

            except grpc.RpcError as e:
                raise e
            else:
                break
        self.start_listening(desc.broadcast)

    def start_listening(self, bdesc):
        from drunc.utils.conf_types import ConfTypes
        self.broadcast = BroadcastHandler(
            bdesc,
            ConfTypes.Protobuf
        )


    def close(self):
        pass

    def propagate_command(self, command, data, token):
        response = send_command(
            controller = self.controller,
            token = token,
            command = command,
            rethrow = True,
            data = data
        )

        return response
