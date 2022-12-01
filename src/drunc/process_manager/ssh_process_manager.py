import grpc
import sh

from drunc.communication.process_manager_pb2 import BootRequest, ProcessUUID, ProcessMetadata, ProcessInstance,ProcessInstanceList, ProcessDescription, ProcessRestriction
from drunc.process_manager.process_manager import ProcessManager


class SSHProcessManager(ProcessManager):
    def __init__(self):
        self.process_store       = {} # dict[str, sh.RunningCommand]
        self.process_description = {} # dict[str, ProcessDescription]
        self.process_restriction = {} # dict[str, ProcessRestriction]

        from drunc.utils.utils import setup_fancy_logging
        self.log = setup_fancy_logging('ssh-process-manager')

    def __del__(self):
        self.log.warning('Killing all the known processes before exiting')
        for uuid, process in self.process_store.items():
            if not process.is_alive():
                continue
            self.log.warning(f'Killing {self.process_metadata[uuid]}')
            process.terminate()


    def boot(self, boot_request:BootRequest, context: grpc.aio.ServicerContext=None) -> ProcessUUID:
        import uuid
        this_uuid = str(uuid.uuid4())
        self.log.info(f'Booting {boot_request.process_description.metadata}')

        def process_output(line):
            self.log.debug(line)

        if len(boot_request.process_restriction.allowed_hosts) < 1:
            raise RuntimeError('No allowed host provided! bailing')

        for host in boot_request.process_restriction.allowed_hosts:
            try:
                user = boot_request.process_description.metadata.user
                user_host = host if not user else f'{user}@{host}'

                env_var = boot_request.process_description.env
                cmd =';'.join([ f"export {n}=\"{v}\"" for n,v in env_var.items()])

                runtime_var = boot_request.process_description.env
                cmd +='; ' + ';'.join([ f"export {n}=\"{v}\"" for n,v in runtime_var.items()])

                exe_arg = boot_request.process_description.executable_and_arguments
                cmd += '; '

                for exe, args in exe_arg.items():
                    cmd += exe
                    for arg in args.values:
                        cmd += f' {arg}'
                    cmd += '; '

                arguments = [user_host, "-tt", "-o StrictHostKeyChecking=no", cmd]

                self.process_store[this_uuid] = sh.ssh (
                    *arguments,
                    _out=process_output,
                    _bg=True,
                    _bg_exc=False,
                    _new_session=True,
                )
                break
            except Exception as e:
                print(f'Couldn\'t start on host {host}, reason:\n{str(e)}')
                print(f'\nTrying on a different host')
                continue

        self.process_description[this_uuid] = ProcessDescription()
        self.process_description[this_uuid].CopyFrom(boot_request.process_description)
        self.process_description[this_uuid].metadata.uuid.uuid = this_uuid
        self.process_restriction[this_uuid] = ProcessRestriction()
        self.process_restriction[this_uuid].CopyFrom(boot_request.process_restriction)

        self.log.info(f'Booted {boot_request.process_description.metadata.name} uuid: {this_uuid}')
        uid = ProcessUUID(
            uuid = this_uuid
        )
        return uid

    def list_process(self, process_selector:ProcessMetadata, context: grpc.aio.ServicerContext=None) -> ProcessInstanceList:
        ret = []
        import re
        uuid_selector = '.*' if not process_selector.uuid.uuid else process_selector.uuid.uuid
        user_selector = '.*' if not process_selector.user      else process_selector.user
        name_selector = '.*' if not process_selector.name      else process_selector.name
        part_selector = '.*' if not process_selector.partition else process_selector.partition

        for uuid, process in self.process_store.items():

            if not re.search(uuid_selector, uuid): continue
            if not re.search(part_selector, self.process_description[uuid].metadata.partition): continue
            if not re.search(user_selector, self.process_description[uuid].metadata.user): continue
            if not re.search(name_selector, self.process_description[uuid].metadata.name): continue

            pd = ProcessDescription()
            pd.CopyFrom(self.process_description[uuid])
            pr = ProcessRestriction()
            pr.CopyFrom(self.process_restriction[uuid])
            pu = ProcessUUID(uuid=uuid)
            pi = ProcessInstance(
                process_description = pd,
                process_restriction = pr,
                status_code = ProcessInstance.StatusCode.RUNNING if process.is_alive() else ProcessInstance.StatusCode.DEAD,
                uuid = pu
            )

            ret += [pi]

        pil = ProcessInstanceList(
            values=ret
        )

        return pil

    def resurrect(self, uuid:ProcessUUID, context: grpc.aio.ServicerContext=None) -> ProcessInstance:
        pass

    def restart(self, uuid:ProcessUUID, context: grpc.aio.ServicerContext=None) -> ProcessInstance:
        pass

    def is_alive(self, uuid:ProcessUUID, context: grpc.aio.ServicerContext=None) -> ProcessInstance:
        pass

    def kill(self, uuid:ProcessUUID, context: grpc.aio.ServicerContext=None) -> ProcessInstance:
        self.log.info(f'Killing {uuid.uuid}')

        if not uuid.uuid in self.process_store:
            raise RuntimeError(f'The process {uuid.uuid} doesn\'t exist!')

        process = self.process_store[uuid.uuid]
        if not process.is_alive():
            raise RuntimeError(f'The process {uuid.uuid} is already dead!')
        process.terminate()

        pd = ProcessDescription()
        pd.CopyFrom(self.process_description[uuid.uuid])
        pr = ProcessRestriction()
        pr.CopyFrom(self.process_restriction[uuid.uuid])
        pu = ProcessUUID()
        pu.CopyFrom(uuid)
        pi = ProcessInstance(
            process_description = pd,
            process_restriction = pr,
            status_code = ProcessInstance.StatusCode.DEAD,
            uuid = pu
        )
        return pi

    def poll(self, uuid:ProcessUUID, context: grpc.aio.ServicerContext=None) -> ProcessInstance:
        pass
