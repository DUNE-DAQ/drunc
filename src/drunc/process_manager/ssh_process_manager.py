import grpc
import sh
from functools import partial

from drunc.communication.process_manager_pb2 import BootRequest, ProcessQuery, ProcessUUID, ProcessMetadata, ProcessInstance, ProcessInstanceList, ProcessDescription, ProcessRestriction, LogRequest, LogLine
from drunc.process_manager.process_manager import ProcessManager


class SSHProcessManager(ProcessManager):
    def __init__(self, conf):
        self.process_store = {} # dict[str, sh.RunningCommand]
        self.boot_request = {} # dict[str, BootRequest]
        # use conf if needed

        from drunc.utils.utils import setup_fancy_logging
        self.log = setup_fancy_logging('ssh-process-manager')
        self.children_logs_depth = 1000
        self.children_logs = {}
        
    def __del__(self):
        self.log.warning('Killing all the known processes before exiting')
        for uuid, process in self.process_store.items():
            if not process.is_alive():
                continue
            self.log.warning(f'Killing {self.boot_request.process_description.metadata[uuid].name}')
            process.terminate()

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
        if query.partition != '': part_selector = query.partition

        processes = []
        all_the_uuids = self.process_store.keys() if not in_boot_request else self.boot_request.keys()
        for uuid in all_the_uuids:

            if not re.search(uuid_selector, uuid): continue
            if not re.search(part_selector, self.boot_request[uuid].process_description.metadata.partition): continue
            if not re.search(user_selector, self.boot_request[uuid].process_description.metadata.user): continue
            if not re.search(name_selector, self.boot_request[uuid].process_description.metadata.name): continue

            processes.append(uuid)

        return processes
    
    def _process_children_logs(self, uuid, line):
        if not uuid in self.children_logs:
            from collections import deque
            self.children_logs[uuid] = deque()

        if len(self.children_logs[uuid]) > self.children_logs_depth:
            self.children_logs[uuid].popleft()

        self.children_logs[uuid].append(line)
        
    def flush(self, query:ProcessQuery,  context: grpc.aio.ServicerContext=None) -> ProcessInstanceList:
        ret = []

        for uuid in self._get_process_uid(query):

            pd = ProcessDescription()
            pd.CopyFrom(self.boot_request[uuid].process_description)
            pr = ProcessRestriction()
            pr.CopyFrom(self.boot_request[uuid].process_restriction)
            pu = ProcessUUID(uuid=uuid)
            return_code = 0
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
        
        
        
    def logs(self, log_request:LogRequest,  context: grpc.aio.ServicerContext=None) -> LogLine:
        uid = self._ensure_one_process(self._get_process_uid(log_request.query))
        cursor = -log_request.how_far
        
        if uid in self.children_logs:
            if -cursor > len(self.children_logs[uid]):
                cursor = -len(self.children_logs[uid])

            while cursor != 0:
                ll = LogLine(
                    uuid = ProcessUUID(uuid=uid),
                    line = self.children_logs[uid][cursor]
                )
                yield ll
                cursor += 1
        else:
            ll = LogLine(line='empty')
            yield ll
        
        
    def _boot(self, boot_request:BootRequest, uuid:str) -> ProcessUUID:
        self.log.info(f'Booting {boot_request.process_description.metadata}')

        if len(boot_request.process_restriction.allowed_hosts) < 1:
            raise RuntimeError('No allowed host provided! bailing')

        error = ''

        for host in boot_request.process_restriction.allowed_hosts:
            try:
                user = boot_request.process_description.metadata.user
                user_host = host if not user else f'{user}@{host}'

                env_var = boot_request.process_description.env
                cmd = ';'.join([ f"export {n}=\"{v}\"" for n,v in env_var.items()])

                runtime_var = boot_request.process_description.env
                cmd += ';' + ';'.join([ f"export {n}=\"{v}\"" for n,v in runtime_var.items()])

                cmd += ';'

                for exe_arg in boot_request.process_description.executable_and_arguments:
                    cmd += exe_arg.exec
                    for arg in exe_arg.args:
                        cmd += f' {arg}'
                    cmd += ';'

                arguments = [user_host, "-tt", "-o StrictHostKeyChecking=no", cmd]

                self.process_store[uuid] = sh.ssh (
                    *arguments,
                    _out=partial(self._process_children_logs, uuid),
                    _bg=True,
                    _bg_exc=False,
                    _new_session=True,
                )
                self.log.info(f'Command:\nssh \'{" ".join(arguments)}\'')
                break
            except Exception as e:
                error = str(e)
                print(f'Couldn\'t start on host {host}, reason:\n{str(e)}')
                print(f'\nTrying on a different host')
                continue

        if uuid in self.process_store and self.process_store[uuid].is_alive():
            self.boot_request[uuid] = BootRequest()
            self.boot_request[uuid].CopyFrom(boot_request)
            
            self.log.info(f'Booted {boot_request.process_description.metadata.name} uid: {uuid}')
            uid = ProcessUUID(
                uuid = uuid
            )
            return uid
        else:
            raise RuntimeError(f'Couldn\'t boot {boot_request.process_description.metadata.name}, reason: {error}')


    def list_process(self, query:ProcessQuery, context: grpc.aio.ServicerContext=None) -> ProcessInstanceList:
        ret = []
        for uuid in self._get_process_uid(query):

            pd = ProcessDescription()
            pd.CopyFrom(self.boot_request[uuid].process_description)
            pr = ProcessRestriction()
            pr.CopyFrom(self.boot_request[uuid].process_restriction)
            pu = ProcessUUID(uuid=uuid)
            return_code = 0
            if not self.process_store[uuid].is_alive():
                try:
                    return_code = self.process_store[uuid].exit_code
                except Exception as e:
                    return_code = e.exit_code
            pi = ProcessInstance(
                process_description = pd,
                process_restriction = pr,
                status_code = ProcessInstance.StatusCode.RUNNING if self.process_store[uuid].is_alive() else ProcessInstance.StatusCode.DEAD,
                return_code = return_code,
                uuid = pu
            )
            ret += [pi]

        pil = ProcessInstanceList(
            values=ret
        )

        return pil


    def boot(self, boot_request:BootRequest, context: grpc.aio.ServicerContext=None) -> ProcessUUID:
        import uuid
        this_uuid = str(uuid.uuid4())
        return self._boot(boot_request, this_uuid)

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
    
    def restart(self, query:ProcessQuery, context: grpc.aio.ServicerContext=None) -> ProcessInstanceList:
        uuids = self._get_process_uid(query, in_boot_request=True)
        uuid = self._ensure_one_process(uuids, in_boot_request=True)

        if uuid in self.process_store:
            process = self.process_store[uuid]
            if process.is_alive():
                process.terminate()

        return self._boot(self.boot_request[uuid], uuid)

    def is_alive(self, query:ProcessQuery, context: grpc.aio.ServicerContext=None) -> ProcessInstanceList:
        uuids = self._get_process_uid(query)
        uuid = self._ensure_one_process(uuids)
        
        process = self.process_store[uuid]
        is_alive = process.is_alive()
        return_code = process.exit_code if not is_alive else 0

        pd = ProcessDescription()
        pd.CopyFrom(self.boot_request[uuid].process_description)
        pr = ProcessRestriction()
        pr.CopyFrom(self.boot_request[uuid].process_restriction)
        pu = ProcessUUID(uuid=uuid)
    
        pi = ProcessInstance(
            process_description = pd,
            process_restriction = pr,
            status_code = ProcessInstance.StatusCode.RUNNING if is_alive else ProcessInstance.StatusCode.DEAD,
            return_code = return_code,
            uuid = pu,
        )
        return pi

    def kill(self, query:ProcessQuery, context: grpc.aio.ServicerContext=None) -> ProcessInstance:
        uuids = self._get_process_uid(query)
        uuid = self._ensure_one_process(uuids)

        process = self.process_store[uuid]
        if not process.is_alive():
            raise RuntimeError(f'The process {uuid} is already dead!')

        process.terminate()

        pd = ProcessDescription()
        pd.CopyFrom(self.boot_request[uuid].process_description)
        pr = ProcessRestriction()
        pr.CopyFrom(self.boot_request[uuid].process_restriction)
        pu = ProcessUUID(uuid=uuid)
        pi = ProcessInstance(
            process_description = pd,
            process_restriction = pr,
            status_code = ProcessInstance.StatusCode.DEAD,
            uuid = pu
        )
        del self.process_store[uuid]
        return pi

    
    def killall(self, query:ProcessQuery, context: grpc.aio.ServicerContext=None) -> ProcessInstanceList:
        uuids = self._get_process_uid(query)
        ret = []

        for uuid in uuids:
            process = self.process_store[uuid]
            if not process.is_alive():
                if query.force:
                    continue
                else:
                    raise RuntimeError(f'The process {uuid} is already dead!')

            process.terminate()

            pd = ProcessDescription()
            pd.CopyFrom(self.boot_request[uuid].process_description)
            pr = ProcessRestriction()
            pr.CopyFrom(self.boot_request[uuid].process_restriction)
            pu = ProcessUUID(uuid= uuid)
            ret += [
                ProcessInstance(
                    process_description = pd,
                    process_restriction = pr,
                    status_code = ProcessInstance.StatusCode.DEAD,
                    uuid = pu
                )
            ]
            del self.process_store[uuid]

        pil = ProcessInstanceList(
            values=ret
        )
        return pil
