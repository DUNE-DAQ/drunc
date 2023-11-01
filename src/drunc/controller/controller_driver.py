from druncschema.request_response_pb2 import Request, Response, Description

from drunc.utils.grpc_utils import unpack_any
from drunc.utils.shell_utils import GRPCDriver

class ConfigurationTypeNotSupported(Exception):
    def __init__(self, conf_type):
        self.type = conf_type
        super(ConfigurationTypeNotSupported, self).__init__(
            f'{str(conf_type)} is not supported by this controller'
        )

class ControllerDriver(GRPCDriver):
    def __init__(self, address:str, token):
        super(ControllerDriver, self).__init__(
            name = 'controller_driver',
            address = address,
            token = token
        )

    def create_stub(self, channel):
        from druncschema.controller_pb2_grpc import ControllerStub
        return ControllerStub(channel)

    def describe(self) -> Description:
        r = self._create_request(payload = None)
        answer = self.send_command('describe', rethrow=True)
        desc = unpack_any(answer.data, Description)
        return desc

