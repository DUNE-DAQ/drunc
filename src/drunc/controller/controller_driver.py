from druncschema.request_response_pb2 import Request, Response, Description
from druncschema.generic_pb2 import PlainText, PlainTextVector
from druncschema.controller_pb2 import Status, ChildrenStatus

from drunc.utils.grpc_utils import unpack_any
from drunc.utils.shell_utils import GRPCDriver


class ControllerDriver(GRPCDriver):
    def __init__(self, address:str, token, **kwargs):
        super(ControllerDriver, self).__init__(
            name = 'controller_driver',
            address = address,
            token = token,
            **kwargs
        )

    def create_stub(self, channel):
        from druncschema.controller_pb2_grpc import ControllerStub
        return ControllerStub(channel)

    def describe(self) -> Description:
        return self.send_command('describe', outformat = Description)

    def describe_fsm(self) -> Description:
        from druncschema.controller_pb2 import FSMCommandsDescription
        return self.send_command('describe_fsm', outformat = FSMCommandsDescription)

    def ls(self) -> Description:
        return self.send_command('ls', outformat = PlainTextVector)

    def get_status(self) -> Description:
        return self.send_command('get_status', outformat = Status)

    def get_children_status(self) -> Description:
        return self.send_command('get_children_status', outformat = ChildrenStatus)

    def take_control(self) -> Description:
        return self.send_command('take_control', outformat = PlainText)

    def who_is_in_charge(self, rethrow=None) -> Description:
        return self.send_command('who_is_in_charge', outformat = PlainText)

    def surrender_control(self) -> Description:
        return self.send_command('surrender_control')

    def execute_fsm_command(self, arguments) -> Description:
        from druncschema.controller_pb2 import FSMCommandResponse
        return self.send_command('execute_fsm_command', data = arguments, outformat = FSMCommandResponse)

    def include(self, arguments) -> Description:
        from druncschema.controller_pb2 import FSMCommandResponse
        return self.send_command('include', data = arguments, outformat = PlainText)

    def exclude(self, arguments) -> Description:
        from druncschema.controller_pb2 import FSMCommandResponse
        return self.send_command('exclude', data = arguments, outformat = PlainText)





