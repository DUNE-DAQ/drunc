import grpc
from druncschema.generic_pb2 import OutcomeFlag
from druncschema.process_manager_pb2 import LogLines, LogRequest, ProcessQuery
from druncschema.run_control_pb2 import (
    DeploySessionResponseFlag,
    EndSessionRequest,
    EndSessionResponseFlag,
    LogOnServerRequest,
    StartSessionRequest,
    ValidateCommunicationRequest,
    ValidateSessionRequest,
)
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

    def validate_session(
        self,
        process_manager: str,
        path_to_configuration_file: str,
        session_id: str,
        session_name: str,
    ) -> DeploySessionResponseFlag:
        """
        Validate the session's requirements.

        Args:
            process_manager (str): The name of the process manager.
            path_to_configuration_file (str): The path to the configuration file.
            session_id (str): The session ID to validate.
            session_name (str): The name of the session that will be used.

        Returns:
            DeploySessionResponseFlag: The response flag indicating the result of the validation.
        """
        self.log.info("Running validate_session")

        # Construct the request
        request = ValidateSessionRequest(
            token=self.token,
            process_manager=process_manager,
            path_to_configuration_file=path_to_configuration_file,
            session_id=session_id,
            session_name=session_name,
        )

        return self.stub.validate_session(request).result.status

    def start_session(
        self,
        process_manager: str,
        path_to_configuration_file: str,
        session_id: str,
        session_name: str,
        override_logs: bool,
        controller_log_level: str,
        sleep_between_app_boot: float,
    ) -> DeploySessionResponseFlag:
        """
        Start the session.

        Args:
            process_manager (str): The name of the process manager.
            path_to_configuration_file (str): The path to the configuration file.
            session_id (str): The session ID to validate.
            session_name (str): The name of the session that will be used.
            controller_log_level (str): The log level override for the controller.
            sleep_between_app_boot (float): The sleep time between application boots.

        Returns:
            DeploySessionResponseFlag: The response flag indicating the result of the validation.
        """
        self.log.info("Running start_session")

        # Construct the request
        request = StartSessionRequest(
            token=self.token,
            process_manager=process_manager,
            path_to_configuration_file=path_to_configuration_file,
            session_id=session_id,
            session_name=session_name,
            override_logs=override_logs,
            controller_log_level=controller_log_level,
            sleep_between_app_boot=sleep_between_app_boot,
        )

        return self.stub.start_session(request).result.status

    def end_session(self, session_name: str) -> EndSessionResponseFlag:
        """
        End the session.

        Args:
            session_name (str): The name of the session to terminate.

        Returns:
            EndSessionResponseFlag: The response flag indicating the result of ending the session.

        """
        self.log.info("Running end_session")

        # TODO: Implement a check for whether the session exists in the session manager
        # registry

        # Construct the request
        request = EndSessionRequest(token=self.token, session_name=session_name)

        return self.stub.end_session(request).result.status

    def validate_communication(self) -> OutcomeFlag:
        """
        Establish communication with the run control server and validate the connection.

        Args:
            None

        Returns:
            OutcomeFlag: The outcome of the communication validation.

        Raises:
            None
        """
        self.log.info("Running validate_communication")

        # Construct the request
        # TODO:  Add a catch to map the response to the OutcomeFlag enum and handle any
        # gRPC errors that may occur
        request = ValidateCommunicationRequest(token=self.token)

        return self.stub.validate_communication(request).status

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
        return self.stub.log_on_server(request, timeout=timeout).flag

    def logs(self, grep: str, how_far: int, timeout: int | float = 60) -> LogLines:
        """
        Retrieve the logfile contents of the run control server.

        Args:
            grep (str): The message to log.
            how_far (int): The number of lines to retrieve from the log file.

        Returns:
            OutcomeFlag: The outcome of the logging operation.

        Raises:
        """
        # TODO: Implement the gRPC error handling
        self.log.info("Running log_on_server")

        # Construct the request
        request = LogRequest(
            token=self.token,
            query=ProcessQuery(token=self.token, user=self.token.user_name),
            how_far=how_far,
        )
        self.log.warning("Not yet implemented grep, ignoring for now.")
        return self.stub.logs(request, timeout=timeout).flag
