from concurrent import futures

import grpc
from druncschema.description_pb2 import Description
from druncschema.request_response_pb2 import Request, ResponseFlag
from druncschema.session_manager_pb2 import (
    AllActiveSessions,
    AllConfigKeys,
    ConfigKey,
)
from druncschema.session_manager_pb2_grpc import (
    SessionManagerServicer,
    SessionManagerStub,
    add_SessionManagerServicer_to_server,
)
from druncschema.token_pb2 import Token

from drunc.session_manager.session_manager import SessionManager


def start_grpc_server():
    """
    Start a local gRPC server.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_SessionManagerServicer_to_server(
        SessionManager(name="session_manager", configuration=[]),
        server
        )
    
    port = server.add_insecure_port("[::]:0")
    server.start()
    return server, port

def test_describe2():
    token = Token()
    server, port = start_grpc_server()  
    try:
        channel = grpc.insecure_channel(f"localhost:{port}") 
        stub = SessionManagerStub(channel)
        response = stub.describe(Request(token=token))

        assert isinstance(response, Description)
        assert response.name == "session_manager"
        assert response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY
        assert any(cmd.name == "describe" for cmd in response.commands)


    finally:
        server.stop(None) 


def test_list_all_sessions():
    token = Token()
    server, port = start_grpc_server()  

    try:
        channel = grpc.insecure_channel(f"localhost:{port}") 
        stub = SessionManagerStub(channel)
        response = stub.list_all_sessions(Request(token=token))

        assert isinstance(response, AllActiveSessions)

        assert response.name == "session_manager"
        assert response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY

        assert len(response.active_sessions) == 1

        session = response.active_sessions[0]
        assert session.name == "dummy_session"
        assert session.user == "dummy_user"
        assert isinstance(session.config_key, ConfigKey)
        assert session.config_key.file == "dummy_config_file"
        assert session.config_key.session_id == "dummy_config_session_id"


    finally:
        server.stop(None) 


def test_list_all_configs():
    token = Token()
    server, port = start_grpc_server()  


    try:
        with grpc.insecure_channel(f"localhost:{port}") as channel:
            stub = SessionManagerStub(channel)
            response = stub.list_all_configs(Request(token=token))

            assert isinstance(response, AllConfigKeys)

            assert response.name == "session_manager"
            assert response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY

            assert len(response.config_keys) == 17
            config = response.config_keys[0]
            assert config.file == "example-configs.data.xml"
            assert config.session_id == "local-tpreplay-config"

    finally:
        server.stop(None)


class FaultySessionManager(SessionManagerServicer):
    def list_all_configs(self, request, context):
        context.abort(grpc.StatusCode.INTERNAL, "Simulated internal error")


def test_list_all_configs_error_handling():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    add_SessionManagerServicer_to_server(FaultySessionManager(), server)
    port = server.add_insecure_port("[::]:0")
    server.start()

    try:
        with grpc.insecure_channel(f"localhost:{port}") as channel:
            stub = SessionManagerStub(channel)
            try:
                stub.list_all_configs(Request())
                assert False, "Expected RpcError"
            except grpc.RpcError as e:
                assert e.code() == grpc.StatusCode.INTERNAL
                assert "Simulated internal error" in e.details()
    finally:
        server.stop(None)