from druncschema.request_response_pb2 import Request, Response, Description
from druncschema.generic_pb2 import PlainText, PlainTextVector
from druncschema.controller_pb2 import Status, ChildrenStatus

from drunc.utils.shell_utils import GRPCDriver

class ConfigurationTypeNotSupported(Exception):
    def __init__(self, conf_type):
        self.type = conf_type
        super(ConfigurationTypeNotSupported, self).__init__(
            f'{str(conf_type)} is not supported by this controller'
        )

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

    def describe(self, rethrow=None) -> Description:
        return self.send_command('describe', rethrow = rethrow, outformat = Description)

    def describe_fsm(self, rethrow=None) -> Description:
        from druncschema.controller_pb2 import FSMCommandsDescription
        return self.send_command('describe_fsm', rethrow = rethrow, outformat = FSMCommandsDescription)

    def ls(self, rethrow=None) -> Description:
        return self.send_command('ls', rethrow = rethrow, outformat = PlainTextVector)

    def get_status(self, rethrow=None) -> Description:
        return self.send_command('get_status', rethrow = rethrow, outformat = Status)

    def get_children_status(self, rethrow=None) -> Description:
        return self.send_command('get_children_status', rethrow = rethrow, outformat = ChildrenStatus)

    def take_control(self, rethrow=None) -> Description:
        return self.send_command('take_control', rethrow = rethrow, outformat = PlainText)

    def who_is_in_charge(self, rethrow=None) -> Description:
        return self.send_command('who_is_in_charge', rethrow = rethrow, outformat = PlainText)

    def surrender_control(self, rethrow=None) -> Description:
        return self.send_command('surrender_control', rethrow = rethrow)

    def execute_fsm_command(self, data, rethrow=None) -> Description:
        from druncschema.controller_pb2 import FSMCommandResponse
        return self.send_command('execute_fsm_command', data = data, rethrow = rethrow, outformat = FSMCommandResponse)

    def include(self, data, rethrow=None) -> Description:
        from druncschema.controller_pb2 import FSMCommandResponse
        return self.send_command('include', data = data, rethrow = rethrow, outformat = PlainText)

    def exclude(self, data, rethrow=None) -> Description:
        from druncschema.controller_pb2 import FSMCommandResponse
        return self.send_command('exclude', data = data, rethrow = rethrow, outformat = PlainText)





