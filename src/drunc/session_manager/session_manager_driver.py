"""Driver for the session manager service."""

from druncschema.description_pb2 import NewDescription
from druncschema.request_response_pb2 import Description
from druncschema.session_manager_pb2 import AllActiveSessions, AllConfigKeys
from druncschema.session_manager_pb2_grpc import SessionManagerStub
from druncschema.token_pb2 import Token
from grpc import Channel

from drunc.utils.shell_utils import DecodedResponse, GRPCDriver


class SessionManagerDriver(GRPCDriver):
    """Provides an interface to the session manager service.

    This class provides the client-side methods required to interact with a remote
    session manager service, via gRPC connections.
    """

    def __init__(self, address: str, token: Token, **kwargs):
        """Create a new session manager driver instance.

        Args:
            address: The address of the session manager service.
            token: The token for authentication.
            **kwargs: Additional keyword arguments for the driver.
        """
        super().__init__(
            name="session_manager_driver", address=address, token=token, **kwargs
        )

    def create_stub(self, channel: Channel) -> SessionManagerStub:
        """Create gRPC stubs for the session manager service.

        Args:
            channel: The gRPC channel to use for communication.

        Returns:
            An object containing session manager service method stubs.
        """
        return SessionManagerStub(channel)

    def new_describe(self) -> DecodedResponse | None:
        """Describe the session manager service.

        Returns:
            A decoded response object containing the description of the service.
        """
        return self.send_command("new_describe", outformat=NewDescription)

    def describe(self) -> DecodedResponse | None:
        """Describe the session manager service.

        Returns:
            A decoded response object containing the description of the service.
        """
        return self.send_command("describe", outformat=Description)

    def list_all_sessions(self) -> DecodedResponse | None:
        """List all active sessions managed by the session manager.

        Returns:
            A decoded response object containing a list of all active sessions.
        """
        return self.send_command("list_all_sessions", outformat=AllActiveSessions)

    def list_all_configs(self) -> DecodedResponse | None:
        """List all available configurations in the session manager.

        Returns:
            A decoded response object containing all available configuration keys.
        """
        return self.send_command("list_all_configs", outformat=AllConfigKeys)
