"""Provides an interface to the session manager service."""

from druncschema.request_response_pb2 import Description
from druncschema.session_manager_pb2 import AllActiveSessions
from druncschema.session_manager_pb2_grpc import SessionManagerStub

from drunc.utils.shell_utils import DecodedResponse, GRPCDriver


class SessionManagerDriver(GRPCDriver):
    def __init__(self, address: str, token, **kwargs):
        super(SessionManagerDriver, self).__init__(
            name="session_manager_driver", address=address, token=token, **kwargs
        )

    def create_stub(self, channel):
        return SessionManagerStub(channel)

    def describe(self) -> DecodedResponse:
        return self.send_command("describe", outformat=Description)

    def list_all_sessions(self) -> DecodedResponse:
        return self.send_command("list_all_sessions", outformat=AllActiveSessions)

    # TODO: next PR.
    # def list_all_configs(self) -> DecodedResponse:
    #     return self.send_command('list_all_configs', outformat=AllConfigKeys)
