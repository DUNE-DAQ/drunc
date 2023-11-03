from druncschema.request_response_pb2 import Request, Response
from druncschema.token_pb2 import Token
from druncschema.broadcast_pb2 import BroadcastType

from druncschema.process_manager_pb2 import BootRequest, ProcessQuery, ProcessInstance, ProcessRestriction, ProcessDescription, ProcessUUID, ProcessInstanceList, LogRequest
from druncschema.process_manager_pb2_grpc import ProcessManagerServicer
from drunc.broadcast.server.broadcast_sender import BroadcastSender
import abc

from drunc.utils.grpc_utils import unpack_any

from google.protobuf import any_pb2
from google.rpc import code_pb2
from google.rpc import status_pb2
from grpc_status import rpc_status
from google.protobuf.any_pb2 import Any

class ProcessManager(abc.ABC, ProcessManagerServicer, BroadcastSender):

    def __init__(self, pm_conf, name, session=None, **kwargs):

        super(ProcessManager, self).__init__(
            name = name,
            broadcast_configuration = pm_conf['broadcaster'],
            session = session,
            **kwargs
        )

        self.name = name
        self.session = session
        from logging import getLogger
        self.log = getLogger("process_manager")
        # ProcessManagerServicer.__init__(self)

        from drunc.process_manager.configuration import ProcessManagerConfiguration
        self.configuration = ProcessManagerConfiguration(pm_conf)

        #BroadcastSender.__init__(self, self.configuration.get_broadcaster_configuration())

        from drunc.authoriser.dummy_authoriser import DummyAuthoriser
        from druncschema.authoriser_pb2 import ActionType, SystemType, AuthoriserRequest

        self.authoriser = DummyAuthoriser(
            self.configuration.get_authoriser_configuration(), # sloppy way to do this... should be similar to broadcast
            SystemType.PROCESS_MANAGER
        )

        self.process_store = {} # dict[str, sh.RunningCommand]
        self.boot_request = {} # dict[str, BootRequest]

        from druncschema.request_response_pb2 import CommandDescription
        # TODO, probably need to think of a better way to do this?
        # Maybe I should "bind" the commands to their methods, and have something looping over this list to generate the gRPC functions
        # Not particularly pretty...
        self.commands = [
            CommandDescription(
                name = 'describe',
                data_type = ['None'],
                help = 'Describe self (return a list of commands, the type of endpoint, the name and session).',
                return_type = 'request_response_pb2.Description'
            ),

            CommandDescription(
                name = 'kill',
                data_type = ['process_manager_pb2.ProcessQuery'],
                help = 'Kill listed process from the process query input (can be multiple).',
                return_type = 'process_manager_pb2.ProcessInstanceList'
            ),

            CommandDescription(
                name = 'restart',
                data_type = ['process_manager_pb2.ProcessQuery'],
                help = 'Restart the process from the process query (which must correspond to one process).',
                return_type = 'process_manager_pb2.ProcessInstance'
            ),

            CommandDescription(
                name = 'boot',
                data_type = ['generic_pb2.BootRequest','None'],
                help = 'Start a process.',
                return_type = 'process_manager_pb2.ProcessInstance'
            ),

            CommandDescription(
                name = 'flush',
                data_type = ['process_manager_pb2.ProcessQuery'],
                help = 'Remove the processes from the list that are dead',
                return_type = 'process_manager_pb2.ProcessInstanceList'
            ),

            CommandDescription(
                name = 'logs',
                data_type = ['process_manager_pb2.LogRequest'],
                help = 'Returns the logs from the process ( must correspond to one process). Note this is an ASYNC function',
                return_type = 'process_manager_pb2.LogLine'
            ),

            CommandDescription(
                name = 'ps',
                data_type = ['process_manager_pb2.ProcessQuery'],
                help = 'Get the status of the listed process from the process query input (can be multiple).',
                return_type = 'process_manager_pb2.ProcessInstance'
            ),
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
        self._terminate()

    @abc.abstractmethod
    def _terminate(self):
        pass

    def _create_response(self, payload, token):
        new_token = Token()
        new_token.CopyFrom(token)
        data = Any()
        data.Pack(payload)
        return Response(
            token = new_token,
            data = data
        )
    def _create_stream(self, payload, token):
        new_token = Token()
        new_token.CopyFrom(token)
        data = Any()
        data.Pack(payload)
        return Response(
            token = new_token,
            data = data
        )

    def _generic_command(self, request:Request, command:str, req_format, context):
        self.broadcast(
            message = f'User \'{request.token.user_name}\' attempting to execute {command}',
            btype = BroadcastType.ACK
        )
        if not self.authoriser.is_authorised(request.token, command):
            context.abort_with_status(
                rpc_status.to_status(
                    status_pb2.Status(
                        code=code_pb2.PERMISSION_DENIED,
                        message='Unauthorised',
                        details=[],
                    )
                )
            )
            self.log.error(f'Unauthorised attempt to execute \'{command}\' from \'{request.token.user_name}\'')

        data = request.data if request.data else None

        try:
            if request.HasField('data'):# is not None:
                formatted_data = unpack_any(data, req_format)
                self.log.info(f'\'{request.token.user_name}\' executing \'{type(self).__name__}.{command}\'')
                result = getattr(self, command)(formatted_data, context)
                self.log.info(f'\'{type(self).__name__}.{command}\' executed')
            else:
                result = getattr(self, command)(None, context)


        except Exception as e:
            raise e # let gRPC handle it

        return self._create_response(result,request.token)


    async def _generic_command_async(self, request:Request, command:str, req_format, context):

        if not self.authoriser.is_authorised(request.token, command):
            context.abort_with_status(
                rpc_status.to_status(
                    status_pb2.Status(
                        code=code_pb2.PERMISSION_DENIED,
                        message='Unauthorised',
                        details=[],
                    )
                )
            )
            self.log.error(f'Unauthorised attempt to execute \'{command}\' from \'{request.token.user_name}\'')

        data = request.data if request.data else None

        try:
            formatted_data = unpack_any(data, req_format)
            self.log.info(f'\'{request.token.user_name}\' executing \'{type(self).__name__}.{command}\'')
            f = getattr(self, command)
            async for r in f(formatted_data, context):
                s = self._create_stream(r,request.token)
                yield s

            self.log.info(f'\'{type(self).__name__}.{command}\' executed')

        except Exception as e:
            self.log.error(e)
            raise e # let gRPC handle it



    @abc.abstractmethod
    def _boot_impl(self, boot_data, context) -> ProcessUUID:
        raise NotImplementedError

    def boot(self, req:Request, context) -> Response:
        self.log.debug(f'received \'boot\' request \'{req}\'')
        return self._generic_command(req, '_boot_impl', BootRequest, context)


    @abc.abstractmethod
    def _restart_impl(self, process, context) -> ProcessInstance:
        raise NotImplementedError

    def restart(self, req:Request, context)-> Response:
        self.log.debug(f'received \'restart\' request \'{req}\'')
        return self._generic_command(req, '_restart_impl', ProcessQuery, context)


    @abc.abstractmethod
    def _kill_impl(self, process, context) -> Response:
        raise NotImplementedError

    def kill(self, req:Request, context) -> Response:
        self.log.debug(f'received \'kill\' request \'{req}\'')
        return self._generic_command(req, '_kill_impl', ProcessQuery, context)


    @abc.abstractmethod
    def _ps_impl(self, req, context) -> Response:
        raise NotImplementedError

    def ps(self, req:Request, context) -> Response:
        self.log.debug(f'received \'ps\' request \'{req}\'')
        return self._generic_command(req, '_ps_impl', ProcessQuery, context)


    def _flush_impl(self, query, context) -> Response:
        ret = []

        for uuid in self._get_process_uid(query):

            if uuid not in self.boot_request:
                pu = ProcessUUID(uuid=uuid)
                pi = ProcessInstance(
                    process_description = ProcessDescription(),
                    process_restriction = ProcessRestriction(),
                    status_code = ProcessInstance.StatusCode.DEAD,
                    return_code = None,
                    uuid = pu
                )
                ret += [pi]
                continue

            pd = ProcessDescription()
            pd.CopyFrom(self.boot_request[uuid].process_description)
            pr = ProcessRestriction()
            pr.CopyFrom(self.boot_request[uuid].process_restriction)
            pu = ProcessUUID(uuid=uuid)

            return_code = None
            try:
                if not self.process_store[uuid].is_alive(): # OMG!! remove this implementation code
                    return_code = self.process_store[uuid].exit_code
            except Exception as e:
                pass

            if not self.process_store[uuid].is_alive():
                pi = ProcessInstance(
                    process_description = pd,
                    process_restriction = pr,
                    status_code = ProcessInstance.StatusCode.RUNNING if self.process_store[uuid].is_alive() else ProcessInstance.StatusCode.DEAD,
                    return_code = return_code,
                    uuid = pu
                )
                del self.process_store[uuid]
                ret += [pi]

        pil = ProcessInstanceList(
            values=ret
        )
        return pil

    def flush(self, req:Request, context) -> Response:
        self.log.debug(f'received \'flush\' request \'{req}\'')
        return self._generic_command(req, '_flush_impl', ProcessQuery, context)



    def describe(self, request:Request, context) -> Response:
        return self._generic_command(request, '_describe_impl', None, context)

    def _describe_impl(self, _, dummy):
        from druncschema.request_response_pb2 import Description
        from drunc.utils.grpc_utils import pack_to_any

        return Description(
            type = 'process_manager',
            name = self.name,
            session = 'no_session' if not self.session else self.session,
            commands = self.commands,
            broadcast = pack_to_any(self.describe_broadcast()),
        )

    @abc.abstractmethod
    async def _logs_impl(self, req:Request, context) -> Response:
        raise NotImplementedError

    async def logs(self, req:Request, context) -> Response:
        self.log.debug(f'received \'logs\' request \'{req}\'')
        async for r in self._generic_command_async(req, '_logs_impl', LogRequest, context):
            yield r



    def _ensure_one_process(self, uuids:[str], in_boot_request:bool=False) -> str:
        if uuids == []:
            raise RuntimeError(f'The process corresponding to the query doesn\'t exist')
        elif len(uuids)>1:
            raise RuntimeError(f'There are more than 1 processes corresponding to the query')

        if in_boot_request:
            if not uuids[0] in self.boot_request:
                raise RuntimeError(f'Couldn\'t find the process corresponding to the UUID {uuids[0]} in the boot requests')
        else:
            if not uuids[0] in self.process_store:
                raise RuntimeError(f'Couldn\'t find the process corresponding to the UUID {uuids[0]} in the process store')
        return uuids[0]


    def _get_process_uid(self, query:ProcessQuery, in_boot_request:bool=False) -> [str]:
        import re

        uuid_selector = []
        name_selector = query.names
        user_selector = query.user
        session_selector = query.session
        # relevant reading here: https://github.com/protocolbuffers/protobuf/blob/main/docs/field_presence.md

        for uid in query.uuids:
            uuid_selector += [uid.uuid]

        processes = []
        all_the_uuids = self.process_store.keys() if not in_boot_request else self.boot_request.keys()

        for uuid in all_the_uuids:
            accepted = False
            meta = self.boot_request[uuid].process_description.metadata

            if uuid in uuid_selector: accepted = True

            for name_reg in name_selector:
                if re.search(name_reg, meta.name):
                    accepted = True

            if session_selector == meta.session: accepted = True

            if user_selector == meta.user: accepted = True

            if accepted: processes.append(uuid)

        return processes


    @staticmethod
    def get(conf:dict, **kwargs):
        from rich.console import Console
        console = Console()

        if conf['type'] == 'ssh':
            console.print(f'Starting \'SSHProcessManager\'')
            from drunc.process_manager.ssh_process_manager import SSHProcessManager
            return SSHProcessManager(conf, **kwargs)
        else:
            raise RuntimeError(f'ProcessManager type {conf["type"]} is unsupported!')


