from drunc.controller.children_interface.child_node import ChildNode
from drunc.controller.utils import send_command


class gRPCChildNode(ChildNode):
    def __init__(self, name, conf, conf_type, **kwargs):
        super(gRPCChildNode, self).__init__(
            name = name,
            **kwargs
        )
        from logging import getLogger
        self.log = getLogger(f'{name}-grpc-child')

        if conf_type == 'file':
            self.uri = conf['uri']
            #...

            # first add the shell to the controller broadcast list
            from druncschema.controller_pb2_grpc import ControllerStub
            import grpc
            self.channel = grpc.insecure_channel(self.uri)
            self.controller = ControllerStub(self.channel)

        else:
            raise RuntimeError(conf_type+ ' is not supported')


    # def _update_token(self):
    #     from druncschema.request_response_pb2 import Description
    #     desc = Description()

    #     try:
    #         response = send_command(
    #             controller = self.controller,
    #             token = self.token,
    #             command = 'describe',
    #             rethrow = True
    #         )

    #         response.data.Unpack(desc)
    #     except Exception as e:
    #         self.log.error('Could not get the controller\'s status')
    #         self.log.error(e)
    #         raise e


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

            #response.data.Unpack(desc)
        except Exception as e:
            self.log.error('Could not get the controller\'s status')
            self.log.error(e)
            raise e
