"""
Dummy response objects for Session Manager endpoints.
"""

from druncschema.description_pb2 import CommandDescription, Description
from druncschema.request_response_pb2 import ResponseFlag
from druncschema.session_manager_pb2 import (
    ActiveSession,
    AllActiveSessions,
    AllConfigKeys,
    ConfigKey,
)


def expected_config_keys():
    return [
        ConfigKey(file=f"mock_file_{i}.data.xml", session_id=f"session_{j}")
        for i in range(1, 4)
        for j in range(1, 3)
    ]


DUMMY_COMMANDDESCRIPTION_LIST = [
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

DUMMY_DESCRIBE_RESPONSE = Description(
    type="session_manager",
    name="dummy_session",
    commands=DUMMY_COMMANDDESCRIPTION_LIST,
    children=[],
    flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    token=None,
)

DUMMY_CONFIGKEY = ConfigKey(
    file="dummy_config_file", session_id="dummy_config_session_id"
)

DUMMY_ACTIVESESSION = ActiveSession(
    name="dummy_session", user="dummy_user", config_key=DUMMY_CONFIGKEY
)

DUMMY_ALLACTIVESESSIONS_RESPONSE = AllActiveSessions(
    name="dummy_session",
    token=None,
    active_sessions=[DUMMY_ACTIVESESSION],
    flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
)

DUMMY_ALLCONFIGKEYS_RESPONSE = AllConfigKeys(
    name="dummy_session",
    token=None,
    config_keys=expected_config_keys(),
    flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
)
