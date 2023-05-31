import grpc
import sh
from functools import partial

from druncschema.process_manager_pb2 import BootRequest, ProcessQuery, ProcessUUID, ProcessMetadata, ProcessInstance, ProcessInstanceList, ProcessDescription, ProcessRestriction, LogRequest, LogLine
from drunc.process_manager.process_manager import ProcessManager


class SSHProcessManager(ProcessManager):
    def __init__(self, conf):
        super().__init__(conf)

        from drunc.utils.utils import get_logger
        self.log = get_logger('ssh-process-manager')
        self.children_logs_depth = 1000
        self.children_logs = {}

    def _terminate(self):
        self.log.info('Terminating')

        if self.process_store:
            self.log.warning('Killing all the known processes before exiting')
        else:
            self.log.info('No known process to kill before exiting')

        for uuid, process in self.process_store.items():
            if not process.is_alive():
                continue
            self.log.warning(f'Killing {self.boot_request.process_description.metadata[uuid].name}')
            process.terminate()

    def _process_children_logs(self, uuid, line):
        if not uuid in self.children_logs:
            from collections import deque
            self.children_logs[uuid] = deque()

        if len(self.children_logs[uuid]) > self.children_logs_depth:
            self.children_logs[uuid].popleft()

        self.children_logs[uuid].append(line)

    #def __is_alive_ret_code(uuid):
    # try:
    # etc...

    async def _logs_impl(self, log_request:LogRequest,  context: grpc.aio.ServicerContext=None) -> LogLine:
        uid = self._ensure_one_process(self._get_process_uid(log_request.query))
        cursor = -log_request.how_far

        if uid not in self.children_logs:
            ll = LogLine(
                uuid = ProcessUUID(uuid=uid),
                line='<empty>'
            )
            yield ll
        else:
            if -cursor > len(self.children_logs[uid]):
                cursor = -len(self.children_logs[uid])

            while cursor != 0:
                ll = LogLine(
                    uuid = ProcessUUID(uuid=uid),
                    line = self.children_logs[uid][cursor]
                )
                yield ll
                cursor += 1

    def __boot(self, boot_request:BootRequest, uuid:str) -> ProcessInstance:
        self.log.info(f'Booting {boot_request.process_description.metadata}')

        if len(boot_request.process_restriction.allowed_hosts) < 1:
            raise RuntimeError('No allowed host provided! bailing')

        error = ''

        if uuid in self.boot_request:
            raise RuntimeError(f'Process {uuid} already exists!')

        self.boot_request[uuid] = BootRequest()
        self.boot_request[uuid].CopyFrom(boot_request)

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

                if cmd[-1] == ';':
                    cmd = cmd[:-1]

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
                error += str(e)
                print(f'Couldn\'t start on host {host}, reason:\n{str(e)}')
                print(f'\nTrying on a different host')
                continue

        self.log.info(f'Booted {boot_request.process_description.metadata.name} uid: {uuid}')
        pd = ProcessDescription()
        pd.CopyFrom(self.boot_request[uuid].process_description)
        pr = ProcessRestriction()
        pr.CopyFrom(self.boot_request[uuid].process_restriction)
        pu = ProcessUUID(uuid=uuid)

        return_code = None
        alive = False

        if uuid not in self.process_store:
            pi = ProcessInstance(
                process_description = pd,
                process_restriction = pr,
                status_code = ProcessInstance.StatusCode.DEAD, ## should be unknown
                return_code = return_code,
                uuid = pu
            )
            return pi

        try:
            if not self.process_store[uuid].is_alive():
                return_code = self.process_store[uuid].exit_code
            else:
                alive = True

        except Exception as e:
            pass

        pi = ProcessInstance(
            process_description = pd,
            process_restriction = pr,
            status_code = ProcessInstance.StatusCode.RUNNING if alive else ProcessInstance.StatusCode.DEAD,
            return_code = return_code,
            uuid = pu
        )
        return pi


    def _ps_impl(self, query:ProcessQuery, context: grpc.aio.ServicerContext=None) -> ProcessInstanceList:
        ret = []

        for uuid in self._get_process_uid(query):

            if uuid not in self.process_store:
                pu = ProcessUUID(uuid=uuid)
                pi = ProcessInstance(
                    process_description = ProcessDescription(),
                    process_restriction = ProcessRestriction(),
                    status_code = ProcessInstance.StatusCode.DEAD, # should be unknown
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
            if not self.process_store[uuid].is_alive():
                try:
                    return_code = self.process_store[uuid].exit_code
                except Exception as e:
                    pass

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


    def _boot_impl(self, boot_request:BootRequest, context: grpc.aio.ServicerContext=None) -> ProcessUUID:
        import uuid
        this_uuid = str(uuid.uuid4())
        return self.__boot(boot_request, this_uuid)



    def _restart_impl(self, query:ProcessQuery, context: grpc.aio.ServicerContext=None) -> ProcessInstanceList:
        uuids = self._get_process_uid(query, in_boot_request=True)
        uuid = self._ensure_one_process(uuids, in_boot_request=True)

        if uuid in self.process_store:
            process = self.process_store[uuid]
            if process.is_alive():
                process.terminate()

        return self.__boot(self.boot_request[uuid], uuid)


    def _kill_impl(self, query:ProcessQuery, context: grpc.aio.ServicerContext=None) -> ProcessInstance:
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


    def _killall_impl(self, query:ProcessQuery, context: grpc.aio.ServicerContext=None) -> ProcessInstanceList:
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
