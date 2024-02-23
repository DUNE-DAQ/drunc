from drunc.controller.children_interface.child_node import ChildNode, ChildNodeType
from drunc.controller.utils import send_command
from drunc.broadcast.client.broadcast_handler import BroadcastHandler
from drunc.utils.configuration_utils import ConfTypes, ConfData, ConfTypeNotSupported
import grpc as grpc

class gRPCChildNode(ChildNode):
    def __init__(self, name, configuration:ConfData, **kwargs):
        super().__init__(
            name = name,
            node_type = ChildNodeType.gRPC
        )
        from logging import getLogger
        self.log = getLogger(f'{name}-grpc-child')

        if child_conf.type != ConfTypes.RawDict:
            raise ConfTypeNotSupported(child_conf.type, 'gRPCChildNode')

        self.uri = child_conf['uri']


        from druncschema.controller_pb2_grpc import ControllerStub
        import grpc
        self.channel = grpc.insecure_channel(self.uri)
        self.controller = ControllerStub(self.channel)

        from druncschema.request_response_pb2 import Description
        desc = Description()
        ntries = 5
        from drunc.utils.grpc_utils import ServerUnreachable
        from drunc.exceptions import DruncSetupException

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
                    self.log.error(f'Could not connect to the controller ({self.uri}), trial {itry+1} of {ntries}')
                    from time import sleep
                    sleep(5)

            except grpc.RpcError as e:
                raise DruncSetupException from e
            else:
                self.log.info(f'Connected to the controller ({self.uri})!')
                break
        self.start_listening(
            ConfData(
                type = ConfTypes.ProtobufObject,
                data = desc.broadcast
            )
        )

    def start_listening(self, bdesc):
        self.broadcast = BroadcastHandler(
            bdesc,
        )

    def get_status(self, token):
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
        pass

    def propagate_command(self, command, data, token):
        success = False
        try:
            response = send_command(
                controller = self.controller,
                token = token,
                command = command,
                rethrow = True,
                data = data
            )
            success = True
        except Exception as e:
            if command != 'execute_fsm_command':
                raise e


        if command == 'execute_fsm_command':
            from druncschema.controller_pb2 import FSMCommandResponseCode
            return FSMCommandResponseCode.SUCCESSFUL if success else FSMCommandResponseCode.SUCCESSFUL
        else:
            return response

