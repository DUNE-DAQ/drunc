"""The session manager service."""

import abc
from os import getenv
from pathlib import Path

from conffwk import Configuration
from druncschema.description_pb2 import CommandDescription, Description
from druncschema.request_response_pb2 import (
    Request,
    ResponseFlag,
)
from druncschema.session_manager_pb2 import (
    ActiveSession,
    AllActiveSessions,
    AllConfigKeys,
    ConfigKey,
)
from druncschema.session_manager_pb2_grpc import SessionManagerServicer
from grpc import ServicerContext

from drunc.exceptions import DruncSetupException
from drunc.session_manager.configuration import SessionManagerConfHandler

# from drunc.utils.grpc_utils import respond_with_rich_error_status, abort_with_rich_error
from drunc.utils.utils import get_logger, pid_info_str
from google.rpc import code_pb2

class SessionManager(abc.ABC, SessionManagerServicer):
    """Provides a gRPC service to manage and interact with sessions.

    This class implements the server-side session manager logic, used to create
    and manage sessions.
    """

    def __init__(self, name: str, configuration: SessionManagerConfHandler):
        """Create a new session manager instance.

        Args:
            name: The name of the session manager.
            configuration: The configuration handler for the session manager.
        """
        super().__init__()

        self.log = get_logger("session_manager")
        self.log.debug(pid_info_str())
        self.log.debug("Initialized SessionManager")

        self.name = name
        self.configuration = configuration

    def describe(self, request: Request, context: ServicerContext) -> Description:
        """Respond with a description of this session manager service.

        Args:
            request: The incoming request (not used).
            context: The gRPC context (not used).

        Returns:
            A response containing the service description.
        """
        self.log.debug(f"{self.name} running describe")

        commands = [
            CommandDescription(
                name="describe",
                data_type=["None"],
                help="List the methods exposed by this endpoint.",
                return_type="description_pb2.Description",
            ),
            CommandDescription(
                name="list_all_sessions",
                data_type=["None"],
                help="List all active sessions.",
                return_type="session_manager_pb2.AllActiveSessions",
            ),
            CommandDescription(
                name="list_all_configs",
                data_type=["None"],
                help="List all available configurations.",
                return_type="session_manager_pb2.AllConfigKeys",
            ),
        ]

        return Description(
            type="session_manager",
            name=self.name,
            commands=commands,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            token=None,
        )

    def list_all_sessions(
        self, request: Request, context: ServicerContext
    ) -> AllActiveSessions:
        """Respond with a list of all active sessions.

        Args:
            request: The incoming request (not used).
            context: The gRPC context (not used).

        Returns:
            A response containing all active sessions.
        """
        self.log.debug(f"{self.name} running list_all_sessions")

        dummy_config = ConfigKey(
            file="dummy_config_file",
            session_id="dummy_config_session_id",
        )

        dummy_session = ActiveSession(
            name="dummy_session",
            user="dummy_user",
            config_key=dummy_config,
        )

        return AllActiveSessions(
            name=self.name,
            token=None,
            active_sessions=[dummy_session],
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

    def list_all_configs(
        self, request: Request, context: ServicerContext
    ) -> AllConfigKeys:
        """Respond with a list of all available configurations.

        Args:
            request: The incoming request (not used).
            context: The gRPC context (not used).

        Returns:
            A response containing all available configuration keys.
        """
        self.log.debug(f"{self.name} running list_all_configs")

        # Get search paths for available configurations.
        search_paths = getenv("DUNEDAQ_DB_PATH")
        if search_paths is None:
            error_msg = "DUNEDAQ_DB_PATH not set"
            self.log.error(error_msg)
            raise DruncSetupException(
                message=error_msg,
                grpc_error_code=code_pb2.FAILED_PRECONDITION,
                detail_type="precondition",
                subject="DUNEDAQ_DB_PATH",
                type="MISSING_OR_INVALID",
                )

        # Find all configuration files.
        config_files: list[Path] = []
        for path in search_paths.split(":"):
            config_glob = Path(path).rglob("*.data.xml")
            config_files.extend(config_glob)

        if not config_files:
            error_msg = "Configuration files not found"
            self.log.error(error_msg)
            raise DruncSetupException(
                message=error_msg,
                grpc_error_code=code_pb2.NOT_FOUND,
                detail_type="resource_info",
                resource_type="SessionConfiguration",
            )

        # Parse all configuration files.
        configs = []
        for file in config_files:
            try:
                config = Configuration(f"oksconflibs:{file}")
            except Exception as e:
                context_msg= f"Configuration parse error in '{str(file)}': {str(e)}"
                self.log.error(context_msg)
                raise DruncSetupException(
                    message=context_msg,
                    grpc_error_code=code_pb2.FAILED_PRECONDITION,
                    detail_type="precondition",
                    type="PARSE_ERROR",
                    subject=str(file),
                )

            # Parse all session configurations in this file.
            try:
                for session_config in config.get_dals("Session"):
                    config_key = ConfigKey(
                        file=file.name,
                        session_id=session_config.id,
                    )
                    configs.append(config_key)
            except Exception as e:
                context_msg = f"Failed to get DALs from {str(file)}: {str(e)}"
                self.log.error(context_msg)
                raise DruncSetupException(
                    message=context_msg,
                    grpc_error_code=code_pb2.FAILED_PRECONDITION,
                    detail_type="precondition",
                    type="DALs_STRUCTURE_INVALID",
                    subject=str(file),
                )

        return AllConfigKeys(
            name=self.name,
            token=None,
            config_keys=configs,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )
