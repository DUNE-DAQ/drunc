from unittest.mock import MagicMock

import grpc
from druncschema.description_pb2 import Description
from druncschema.request_response_pb2 import ResponseFlag
from druncschema.session_manager_pb2 import (
    DESCRIPTOR,
    AllActiveSessions,
    AllConfigKeys,
    ConfigKey,
)
from grpc_testing import server_from_dictionary, strict_real_time

from drunc.session_manager.session_manager import SessionManager

servicers = {
    DESCRIPTOR.services_by_name["SessionManager"]: SessionManager(
        name="dummy_name", 
        configuration=MagicMock())
}


test_server = server_from_dictionary(servicers, strict_real_time())


def test_describe():
    request = SessionManager(
        name="dummy_name", 
        configuration=MagicMock())

    describe_method = test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["SessionManager"].methods_by_name["describe"]),
        request =request,
        invocation_metadata={},
        timeout=1
    )

    response, metadata, code, details = describe_method.termination()

    assert isinstance(response, Description)
    assert code == grpc.StatusCode.OK
    assert response.name == "dummy_name"


def test_list_all_sessions():
    request = SessionManager(
        name="dummy_name", 
        configuration=MagicMock())

    list_all_sessions_method = test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["SessionManager"].methods_by_name["list_all_sessions"]),
        request =request,
        invocation_metadata={},
        timeout=1
    )

    response, metadata, code, details = list_all_sessions_method.termination()

    assert isinstance(response, AllActiveSessions)
    assert code == grpc.StatusCode.OK
    assert response.name == "dummy_name"
    assert response.active_sessions
    
    assert len(response.active_sessions) == 1

    session = response.active_sessions[0]
    assert session.name == "dummy_session"
    assert session.user == "dummy_user"
    assert isinstance(session.config_key, ConfigKey)
    assert session.config_key.file == "dummy_config_file"
    assert session.config_key.session_id == "dummy_config_session_id"


def test_list_all_configs():
    request = SessionManager(
        name="dummy_name", 
        configuration=MagicMock())

    list_all_sessions_method = test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["SessionManager"].methods_by_name["list_all_configs"]
            ),
        request =request,
        invocation_metadata={},
        timeout=1
    )

    response, metadata, code, details = list_all_sessions_method.termination()

    assert isinstance(response, AllConfigKeys)

    assert response.name == "dummy_name"
    assert response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY

    assert len(response.config_keys) == 17
    config = response.config_keys[0]
    assert config.file == "example-configs.data.xml"
    assert config.session_id == "local-tpreplay-config"


