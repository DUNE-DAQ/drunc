import grpc
from druncschema.generic_pb2 import OutcomeFlag
from druncschema.run_control_pb2 import LogOnServerRequest
from druncschema.run_control_pb2_grpc import RunControlStub
from druncschema.token_pb2 import Token

from drunc.utils.grpc_utils import copy_token
from drunc.utils.utils import get_logger


class RunControlDriver:
    def __init__(self, address: str, token: Token):
        self.log = get_logger("run_control.driver")
        self.address = address
        options = [
            ("grpc.keepalive_time_ms", 60000)  # pings the server every 60 seconds
        ]
        self.channel = grpc.insecure_channel(self.address, options=options)
        self.stub = RunControlStub(self.channel)
        self.token = copy_token(token)

    def validate_session(self):
        self.log.info("Running validate_session")
        pass

    def start_session(self):
        self.log.info("Running start_session")
        pass

    def end_session(self):
        self.log.info("Running end_session")
        pass

    def validate_communication(self):
        self.log.info("Running validate_communication")
        pass

    def log_on_server(
        self, msg: str, log_level: str = "INFO", timeout: int | float = 60
    ) -> OutcomeFlag:
        """
        Log a message on the server with the specified log level.

        Args:
            msg (str): The message to log.
            log_level (str): The log level (e.g., "INFO", "ERROR", "DEBUG").

        Returns:
            OutcomeFlag: The outcome of the logging operation.

        Raises:
        """
        # TODO: Implement the gRPC error handling
        self.log.info("Running log_on_server")

        # Construct the request
        request = LogOnServerRequest(token=self.token, text=msg, severity=log_level)
        response = self.stub.log_on_server(request, timeout=timeout)
        return response.flag
