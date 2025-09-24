from typing import List, Tuple
import time
from drunc.tests.grpc.test_pb2 import (DummyRequest, DummyResponse)
from drunc.tests.grpc.test_pb2_grpc import (RootControllerServiceStub)
from drunc.tests.grpc.grpc_log_util import stderr_observer

class IndependentRootControllerClient:
    """
    Managed direct gRPC client connection to RootController server.

    Uses file descriptor level stderr redirection to capture gRPC absl logging
    output that bypasses Python's sys.stderr object.
    """

    def __init__(
        self,
        client_id: str,
        root_port: int,
        client_options: List[Tuple[str, any]] = None,
    ):
        """
        Initialise DirectRootClient.

        Args:
            client_id: Unique identifier for this client instance
            root_port: Port number of the RootController server
            client_options: List of gRPC client configuration options
        """
        self.client_id = client_id
        self.root_port = root_port
        self.client_options = client_options or []

        # Connection state
        self.channel = None
        self.stub = None
        self.log_file = None
        self._connected = False

    def _connect_with_stderr_redirect(self, log_file: str):
        """
        Internal method to establish connection with file descriptor level stderr redirection.

        Args:
            log_file: Path to log file for stderr redirection

        Raises:
            RuntimeError: If connection fails
        """
        if self._connected:
            return

        self.log_file = log_file
        stderr_observer(log_file)
        import grpc

        try:
            self.channel = grpc.insecure_channel(
                f"localhost:{self.root_port}", options=self.client_options
            )
            self.stub = RootControllerServiceStub(self.channel)
            self._connected = True

        except Exception as e:
            raise RuntimeError(
                f"Failed to establish gRPC connection for {self.client_id}: {e}"
            )

    def make_request(self, message: str) -> DummyResponse:
        """
        Send request to RootController server.

        Args:
            message: Message to include in the request

        Returns:
            Response from the RootController server

        Raises:
            RuntimeError: If not connected
        """
        if not self._connected or not self.stub:
            raise RuntimeError(f"DirectRootClient {self.client_id} is not connected")

        request = DummyRequest(
            message=message,
            timestamp=int(time.time() * 1000),
        )

        return self.stub.MakeRequest(request)

    def _disconnect(self):
        """Internal method to close connection and restore stderr."""
        if self.channel:
            self.channel.close()
            self.channel = None
            self.stub = None

        self._connected = False

    def get_log_file(self) -> str:
        """Get path to log file where client errors are written."""
        return self.log_file

    def is_connected(self) -> bool:
        """Check if client is currently connected."""
        return self._connected

    def get_id(self) -> str:
        """Get unique identifier for this client."""
        return self.client_id
