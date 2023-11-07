from druncschema.request_response_pb2 import Request, Response
from druncschema.token_pb2 import Token
from druncschema.broadcast_pb2 import BroadcastType

from druncschema.session_manager_pb2_grpc import SessionManagerServicer
from drunc.broadcast.server.broadcast_sender import BroadcastSender

from drunc.utils.grpc_utils import unpack_any


class SessionManager(SessionManagerServicer, BroadcastSender):

    def authentified_and_authorised():
        def check_token(self, request):


    def __init__(self, sm_conf, name, **kwargs):
        super(SessionManager, self).__init__(
            name = name,
            broadcast_configuration = sm_conf['broadcaster'],
            **kwargs
        )
        self.name = name
        self.session = None

        from logging import getLogger
        self.log = getLogger("session_manager")

        from drunc.authoriser.dummy_authoriser import DummyAuthoriser
        from druncschema.authoriser_pb2 import ActionType, SystemType, AuthoriserRequest

        self.authoriser = DummyAuthoriser(
            self.configuration.get_authoriser_configuration(), # sloppy way to do this... should be similar to broadcast
            SystemType.PROCESS_MANAGER
        )

        self.session_store = {} # dict[str, str]

        from druncschema.request_response_pb2 import CommandDescription
        self.commands = [
        ]

        self.broadcast(
            message = 'ready',
            btype = BroadcastType.SERVER_READY
        )

    def terminate(self):
        self.broadcast(
            message='over_and_out',
            btype=BroadcastType.SERVER_SHUTDOWN
        )

    @authentified_and_authorised(action='read')
    @utils.payload_decoded()
    def describe(self, req:Request):
        self.generic_command(req)


    def list_session(self, req:Request):
        self.generic_command(req)


    def start_session(self, req:Request):
        self.generic_command(req)


    def terminate_session(self, req:Request):
        self.generic_command(req)


    def load_configuration(self, req:Request):
        self.generic_command(req)


    def list_configuration(self, req:Request):
        self.generic_command(req)


