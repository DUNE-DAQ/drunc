from drunc.controller.children_interface.child_node import ChildNode
# from drunc.controller.utils import send_command
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

        from drunc.controller.controller_driver import ControllerDriver
        self.driver = ControllerDriver(
            address = self.uri,
            token = self.token
        )

        ntries = 5
        desc = None

        from drunc.utils.grpc_utils import ServerUnreachable
        from drunc.exceptions import DruncSetupException


        for itry in range(ntries):
            try:
                desc = self.driver.describe(
                    rethrow = True,
                )
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
        self.start_listening(desc.broadcast)

    def start_listening(self, bdesc):
        from drunc.utils.conf_types import ConfTypes
        self.broadcast = BroadcastHandler(
            bdesc,
            ConfTypes.Protobuf
        )

    def get_status(self, token):
        self.driver.token = token
        return self.driver.get_status(
            rethrow=True,
        )

    def terminate(self):
        pass

    def propagate_command(self, command, token, **kwargs):
        success = False
        self.driver.token = token

        try:
            response = getattr(self.driver, command)(
                rethrow=True,
                **kwargs
            )

            # response = send_command(
            #     controller = self.controller,
            #     token = token,
            #     command = command,
            #     rethrow = True,
            #     data = data
            # )
            success = True
        except Exception as e:
            if command != 'execute_fsm_command':
                raise e


        if command == 'execute_fsm_command':
            from druncschema.controller_pb2 import FSMCommandResponseCode
            return FSMCommandResponseCode.SUCCESSFUL if success else FSMCommandResponseCode.SUCCESSFUL
        else:
            return response

