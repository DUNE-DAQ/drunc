"""Driver for the session manager service."""

import grpc
from druncschema.description_pb2 import Description
from druncschema.session_manager_pb2 import AllActiveSessions, AllConfigKeys
from druncschema.session_manager_pb2_grpc import SessionManagerStub
from druncschema.token_pb2 import Token
from google.protobuf.empty_pb2 import Empty

from drunc.utils.shell_utils import GRPCDriver


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
        self.address = address
        self.channel = grpc.insecure_channel(self.address)
        self.stub = SessionManagerStub(self.channel)

    def describe(self) -> Description:
        """Describe the session manager service.

        Returns:
            A response containing the description of the service.
        """

        try:
            response = self.stub.describe(Empty())
        except grpc.RpcError as e:
            self.__handle_grpc_error(e, "describe")

        return response

    def list_all_sessions(self) -> AllActiveSessions:
        """List all active sessions managed by the session manager.

        Returns:
            A response containing a list of all active sessions.
        """

        try:
            response = self.stub.list_all_sessions(Empty())
        except grpc.RpcError as e:
            self.__handle_grpc_error(e, "list_all_sessions")

        return response

    def list_all_configs(self) -> AllConfigKeys:
        """List all available configurations in the session manager.

        Returns:
            A response containing all available configuration keys.
        """

        try:
            response = self.stub.list_all_configs(Empty())
        except grpc.RpcError as e:
            self.__handle_grpc_error(e, "list_all_configs")

        return response
