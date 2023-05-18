from druncschema.request_response_pb2 import Request, Response, Stream
from druncschema.token_pb2 import Token

from druncschema.process_manager_pb2 import BootRequest, ProcessQuery, ProcessInstance, ProcessRestriction, ProcessDescription, ProcessUUID, ProcessInstanceList, LogRequest
from druncschema.process_manager_pb2_grpc import ProcessManagerServicer
import abc

from drunc.utils.grpc_utils import unpack_any

from google.protobuf import any_pb2
from google.rpc import code_pb2
from google.rpc import error_details_pb2
from google.rpc import status_pb2
from grpc_status import rpc_status
from google.protobuf.any_pb2 import Any

class ProcessManager(abc.ABC, ProcessManagerServicer):

    def __init__(self, configuration_loc):
        from drunc.process_manager.configuration import ProcessManagerConfiguration
        self.configuration = ProcessManagerConfiguration(configuration_loc)

        from drunc.authoriser.dummy_authoriser import DummyAuthoriser
        from druncschema.authoriser_pb2 import ActionType, SystemType, AuthoriserRequest

        self.authoriser = DummyAuthoriser(self.configuration.data['authoriser'], SystemType.PROCESS_MANAGER)

        self.process_store = {} # dict[str, sh.RunningCommand]
        self.boot_request = {} # dict[str, BootRequest]
        # use conf if needed

    def terminate(self):
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
        return Stream(
            token = new_token,
            data = data
        )

    def _generic_command(self, request:Request, command:str, req_format, context):

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
            result = getattr(self, command)(formatted_data, context)
            self.log.info(f'\'{type(self).__name__}.{command}\' executed')

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
    def _is_alive_impl(self, process, context) -> Response:
        raise NotImplementedError

    def is_alive(self, req:Request, context) -> Response:
        self.log.debug(f'received \'is_alive\' request \'{req}\'')
        return self._generic_command(req, '_is_alive_impl', ProcessQuery, context)



    @abc.abstractmethod
    def _kill_impl(self, process, context) -> Response:
        raise NotImplementedError

    def kill(self, req:Request, context) -> Response:
        self.log.debug(f'received \'kill\' request \'{req}\'')
        return self._generic_command(req, '_kill_impl', ProcessQuery, context)



    @abc.abstractmethod
    def _killall_impl(self, req, context) -> Response:
        raise NotImplementedError

    def killall(self, req:Request, context) -> Response:
        self.log.debug(f'received \'killall\' request \'{req}\'')
        return self._generic_command(req, '_killall_impl', ProcessQuery, context)



    @abc.abstractmethod
    def _list_process_impl(self, req, context) -> Response:
        raise NotImplementedError

    def list_process(self, req:Request, context) -> Response:
        self.log.debug(f'received \'list_process\' request \'{req}\'')
        return self._generic_command(req, '_list_process_impl', ProcessQuery, context)


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
                if not self.process_store[uuid].is_alive():
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



    @abc.abstractmethod
    async def _logs_impl(self, req:Request, context) -> Stream:
        raise NotImplementedError

    async def logs(self, req:Request, context) -> Stream:
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

        uuid_selector = '.*'
        name_selector = '.*'
        user_selector = '.*'
        part_selector = '.*'
        # relevant reading here: https://github.com/protocolbuffers/protobuf/blob/main/docs/field_presence.md
        if query.HasField('uuid'): uuid_selector = query.uuid.uuid
        if query.name != '': name_selector = query.name
        if query.user != '': user_selector = query.user
        if query.session != '': part_selector = query.session

        processes = []
        all_the_uuids = self.process_store.keys() if not in_boot_request else self.boot_request.keys()
        for uuid in all_the_uuids:

            if not re.search(uuid_selector, uuid): continue
            if not re.search(part_selector, self.boot_request[uuid].process_description.metadata.session): continue
            if not re.search(user_selector, self.boot_request[uuid].process_description.metadata.user): continue
            if not re.search(name_selector, self.boot_request[uuid].process_description.metadata.name): continue

            processes.append(uuid)

        return processes


    @staticmethod
    def get(conf:dict):
        from rich.console import Console
        console = Console()

        if conf['type'] == 'ssh':
            console.print(f'Starting \'SSHProcessManager\'')
            from drunc.process_manager.ssh_process_manager import SSHProcessManager
            return SSHProcessManager(conf)
        else:
            raise RuntimeError(f'ProcessManager type {pm_conf_data["type"]} is unsupported!')


