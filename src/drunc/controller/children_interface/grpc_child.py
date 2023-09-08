from drunc.controller.children_interface.child_node import ChildNode
from drunc.controller.utils import send_command
from drunc.broadcast.client.kafka_stdout_broadcast_handler import KafkaStdoutBroadcastHandler


class gRPCChildNode(ChildNode):
    def __init__(self, name, child_conf, conf_type, **kwargs):
        super(gRPCChildNode, self).__init__(
            name = name,
            **kwargs
        )
        from logging import getLogger
        self.log = getLogger(f'{name}-grpc-child')

        if conf_type == 'file':
            self.uri = child_conf['uri']

        else:
            raise RuntimeError(conf_type+ ' is not supported')

        from druncschema.controller_pb2_grpc import ControllerStub
        import grpc
        self.channel = grpc.insecure_channel(self.uri)
        self.controller = ControllerStub(self.channel)

        from druncschema.request_response_pb2 import Description
        desc = Description()

        response = send_command(
            controller = self.controller,
            token = self.token,
            command = 'describe',
            rethrow = True
        )

        response.data.Unpack(desc)
        self.start_listening(desc.broadcast)

    def close(self):
        pass

    def propagate_command(self, command, data, token):
        try:
            response = send_command(
                controller = self.controller,
                token = token,
                command = command,
                rethrow = True,
                data = data
            )

        except Exception as e:
            self.log.error('Could not get the controller\'s status')
            self.log.error(e)
            raise e

        return response
