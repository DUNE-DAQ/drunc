"""Driver for the session manager service."""

import grpc
from druncschema.description_pb2 import Description
from druncschema.request_response_pb2 import Request
from druncschema.session_manager_pb2 import AllActiveSessions, AllConfigKeys
from druncschema.session_manager_pb2_grpc import SessionManagerStub
from druncschema.token_pb2 import Token

from drunc.utils.grpc_utils import (
    copy_token,
    extract_grpc_rich_error,
    handle_grpc_error,
)
from drunc.utils.utils import get_logger


class SessionManagerDriver:
    """Provides an interface to the session manager service.

    This class provides the client-side methods required to interact with a remote
    session manager service, via gRPC connections.
    """

    def __init__(self, address: str, token: Token):
        """Create a new session manager driver instance.

        Args:
            address: The address of the session manager service.
            token: The token for authentication.
            **kwargs: Additional keyword arguments for the driver.
        """
        self.log = get_logger(
            "controller.interface.SessionManagerDriver"
        )  # TODO: Verify core/interface choice
        self.address = address
        options = [
            ("grpc.keepalive_time_ms", 60000)  # pings the server every 60 seconds
        ]
        self.channel = grpc.insecure_channel(self.address, options=options)
        self.stub = SessionManagerStub(self.channel)
        self.token = copy_token(token)
        self.log = get_logger("session_manager_driver", rich_handler=True)

    def describe(self, timeout: int | float = 60) -> Description:
        """Describe the session manager service.

        Args:
            timeout: The timeout for the gRPC call in seconds.

        Returns:
            A response containing the description of the service.
        """
        request = Request(token=copy_token(self.token))

        try:
            response = self.stub.describe(request, timeout=timeout)
        except grpc.RpcError as e:
            try:
                error_details = extract_grpc_rich_error(e)
                self.log.error(error_details)
            except Exception as extraction_error:
                self.log.debug(
                    f"Could not extract rich error details from gRPC error: {extraction_error}",
                    exc_info=True,
                )

            handle_grpc_error(e)

        return response

    def list_all_sessions(self, timeout: int | float = 60) -> AllActiveSessions:
        """List all active sessions managed by the session manager.

        Args:
            timeout: The timeout for the gRPC call in seconds.

        Returns:
            A response containing a list of all active sessions.
        """
        request = Request(token=copy_token(self.token))

        try:
            response = self.stub.list_all_sessions(request, timeout=timeout)
        except grpc.RpcError as e:
            try:
                error_details = extract_grpc_rich_error(e)
                self.log.error(error_details)
            except Exception as extraction_error:
                self.log.debug(
                    f"Could not extract rich error details from gRPC error: {extraction_error}",
                    exc_info=True,
                )

            handle_grpc_error(e)

        return response

    def list_all_configs(self, timeout: int | float = 60) -> AllConfigKeys:
        """List all available configurations in the session manager.

        Args:
            timeout: The timeout for the gRPC call in seconds.

        Returns:
            A response containing all available configuration keys.
        """
        request = Request(token=copy_token(self.token))

        try:
            response = self.stub.list_all_configs(request, timeout=timeout)
        except grpc.RpcError as e:
            try:
                error_details = extract_grpc_rich_error(e)
                self.log.error(error_details)
            except Exception as extraction_error:
                self.log.debug(
                    f"Could not extract rich error details from gRPC error: {extraction_error}",
                    exc_info=True,
                )

            handle_grpc_error(e)

        return response
