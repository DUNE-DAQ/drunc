"""The session manager service."""

import abc
import logging

from druncschema.request_response_pb2 import (
    CommandDescription,
    Description,
    Response,
    ResponseFlag,
)
from druncschema.session_manager_pb2 import ActiveSession, AllActiveSessions, ConfigKey
from druncschema.session_manager_pb2_grpc import SessionManagerServicer
from druncschema.token_pb2 import Token

from drunc.session_manager.configuration import SessionManagerConfHandler
from drunc.utils.grpc_utils import pack_to_any, unpack_request_data_to
from drunc.utils.utils import pid_info_str


class SessionManager(abc.ABC, SessionManagerServicer):
    def __init__(self, name: str, configuration: SessionManagerConfHandler):
        super().__init__()

        self.log = logging.getLogger("drunc.session_manager")
        self.log.debug(pid_info_str())
        self.log.debug("Initialized SessionManager")

        self.name = name
        self.configuration = configuration

    @unpack_request_data_to(None, pass_token=True)
    def describe(self, token: Token) -> Response:
        self.log.debug(f"{self.name} running describe")

        command_descriptions = [
            CommandDescription(
                name="describe",
                data_type=["None"],
                help="Describe self (return a list of commands, the type of endpoint, the name and session).",
                return_type="request_response_pb2.Description",
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

        description = Description(
            type="session_manager",
            name=self.name,
            session=self.name,
            commands=command_descriptions,
        )

        return Response(
            name=self.name,
            token=None,
            data=pack_to_any(description),
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            children=[],
        )

    @unpack_request_data_to(None, pass_token=True)
    def list_all_sessions(self, token: Token) -> Response:
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

        all_sessions = AllActiveSessions(
            active_sessions=[dummy_session],
        )

        return Response(
            name=self.name,
            token=None,
            data=pack_to_any(all_sessions),
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            children=[],
        )

    # TODO: next PR.
    # @unpack_request_data_to(None, pass_token=True)
    # def list_all_configs(self, token: Token) -> Response:
    #     pass
