import grpc
import sh
from functools import partial
import threading

from druncschema.process_manager_pb2 import BootRequest, ProcessQuery, ProcessUUID, ProcessMetadata, ProcessInstance, ProcessInstanceList, ProcessDescription, ProcessRestriction, LogRequest, LogLine
from drunc.process_manager.process_manager import ProcessManager

# # ------------------------------------------------
# # pexpect.spawn(...,preexec_fn=on_parent_exit('SIGTERM'))
from ctypes import cdll
import signal

# Constant taken from http://linux.die.net/include/linux/prctl.h
PR_SET_PDEATHSIG = 1

from drunc.exceptions import DruncException
class PrCtlError(DruncException):
    pass


def on_parent_exit(signum):
    """
    Return a function to be run in a child process which will trigger
    SIGNAME to be sent when the parent process dies
    """

    def set_parent_exit_signal():
        # http://linux.die.net/man/2/prctl
        result = cdll['libc.so.6'].prctl(PR_SET_PDEATHSIG, signum)
        if result != 0:
            raise PrCtlError('prctl failed with error code %s' % result)
    return set_parent_exit_signal
# ------------------------------------------------

class AppProcessWatcherThread(threading.Thread):
    def __init__(self, pm, name, user, session, process):
        threading.Thread.__init__(self)
        self.pm = pm
        self.user = user
        self.session = session
        self.name = name
        self.process = process

    def run(self):

        exc = None
        try:
            self.process.wait()
        except sh.ErrorReturnCode as e:
            exc = e

        self.pm.notify_join(
            name = self.name,
            session = self.session,
            user = self.user,
            exec = exc
        )

class SSHProcessManager(ProcessManager):
    def __init__(self, configuration, **kwargs):

        import getpass
        self.session = getpass.getuser() # unfortunate

        super().__init__(
            configuration = configuration,
            session = self.session,
            **kwargs
        )

        import logging
        self._log = logging.getLogger('ssh-process-manager')
        # self.children_logs_depth = 1000
        # self.children_logs = {}
        self.watchers = []

        from sh import Command
        self.ssh = Command('/usr/bin/ssh')

    def _terminate(self):
        self._log.info('Terminating')

        if self.process_store:
            self._log.warning('Killing all the known processes before exiting')
        else:
            self._log.info('No known process to kill before exiting')

        for uuid, process in self.process_store.items():
            if not process.is_alive():
                continue
            self._log.warning(f'Killing {self.boot_request[uuid].process_description.metadata.name}')
            process.terminate()


    async def _logs_impl(self, log_request:LogRequest) -> LogLine:
        uid = self._ensure_one_process(self._get_process_uid(log_request.query))
        logfile = self.boot_request[uid].process_description.process_logs_path
        # https://stackoverflow.com/questions/7167008/efficiently-finding-the-last-line-in-a-text-file
        # "Not the straight forward way"...
        import tempfile
        f = tempfile.NamedTemporaryFile(delete=False)
        import sh
        nlines = log_request.how_far
        if not nlines:
            nlines = 100

        try:
            sh.tail(
                f'-{nlines}', logfile,
                _out=f.name,
                _err_to_out=True,
            )
        except Exception as e:
            ll = LogLine(
                uuid = ProcessUUID(uuid=uid),
                line =  f'Could not retrieve logs: {str(e)}'
            )
            yield ll
        f.close()
        with open(f.name, 'r') as fi:
            lines = fi.readlines()
            for line in lines:
                ll = LogLine(
                    uuid = ProcessUUID(uuid=uid),
                    line = line
                )
                yield ll

        import os
        os.remove(f.name)


    def notify_join(self, name, session, user, exec):
        exit_code = None
        if exec:
            exit_code = exec.exit_code
        end_str = f"Process \'{name}\' (session: \'{session}\', user: \'{user}\') process exited with exit code {exit_code}"
        self._log.info(end_str)
        if exec:
            self._log.debug(name+str(exec))

        from druncschema.broadcast_pb2 import BroadcastType
        self.broadcast(
            end_str,
            BroadcastType.SUBPROCESS_STATUS_UPDATE
        )

    def _watch(self, name, session, user, process):
        t = AppProcessWatcherThread(
            pm = self,
            session = session,
            user = user,
            name = name,
            process = process
        )
        t.start()
        self.watchers.append(t)

    def __boot(self, boot_request:BootRequest, uuid:str) -> ProcessInstance:
        self._log.info(f'Booting {boot_request.process_description.metadata}')
        import os
        platform = os.uname().sysname.lower()
        macos = ("darwin" in platform)
        from drunc.exceptions import DruncCommandException

        meta = boot_request.process_description.metadata
        if len(boot_request.process_restriction.allowed_hosts) < 1:
            raise DruncCommandException('No allowed host provided! bailing')

        error = ''

        if uuid in self.boot_request:
            raise DruncCommandException(f'Process {uuid} already exists!')

        self.boot_request[uuid] = BootRequest()
        self.boot_request[uuid].CopyFrom(boot_request)

        for host in boot_request.process_restriction.allowed_hosts:
            try:
                user = boot_request.process_description.metadata.user
                user_host = host if not user else f'{user}@{host}'

                from drunc.utils.utils import now_str
                log_file = boot_request.process_description.process_logs_path
                env_var = boot_request.process_description.env
                cmd = ';'.join([ f"export {n}=\"{v}\"" for n,v in env_var.items()])

                cmd += f'; cd {boot_request.process_description.process_execution_directory} ;'

                for exe_arg in boot_request.process_description.executable_and_arguments:
                    cmd += exe_arg.exec
                    for arg in exe_arg.args:
                        cmd += f' {arg}'
                    cmd += ';'

                if cmd[-1] == ';':
                    cmd = cmd[:-1]

                arguments = [user_host, "-tt", "-o StrictHostKeyChecking=no", f'{{ {cmd} ; }} &> {log_file}']
                # arguments = [user_host, "-tt", "-o StrictHostKeyChecking=no", f'{{ {cmd} ; }} > >(tee -a {log_file}) 2> >(tee -a {log_file} >&2)']
                # I'm gonna bail now and read that log file, anyway, it's probably better that heavy logger applications don't clog up the process manager CPU.
                self.process_store[uuid] = self.ssh (
                    *arguments,
                    # _out=partial(self._process_children_logs, uuid),
                    _bg=True,
                    _bg_exc=False,
                    _new_session=True,
                    _preexec_fn = on_parent_exit(signal.SIGTERM) if not macos else None
                )

                self._watch(
                    name = meta.name,
                    user = meta.user,
                    session = meta.session,
                    process = self.process_store[uuid]
                )
                self._log.info(f'Command:\nssh \'{" ".join(arguments)}\'')
                break

            except Exception as e:
                error += str(e)
                print(f'Couldn\'t start on host {host}, reason:\n{str(e)}')
                print(f'\nTrying on a different host')
                continue

        self._log.info(f'Booted {boot_request.process_description.metadata.name} uid: {uuid}')
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


    def _ps_impl(self, query:ProcessQuery) -> ProcessInstanceList:
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


    def _boot_impl(self, boot_request:BootRequest) -> ProcessInstance:
        import uuid
        this_uuid = str(uuid.uuid4())
        return self.__boot(boot_request, this_uuid)



    def _restart_impl(self, query:ProcessQuery) -> ProcessInstanceList:
        uuids = self._get_process_uid(query, in_boot_request=True)
        uuid = self._ensure_one_process(uuids, in_boot_request=True)

        same_uuid_br = []
        same_uuid_br = BootRequest()
        same_uuid_br.CopyFrom(self.boot_request[uuid])
        same_uuid = uuid

        if uuid in self.process_store:
            process = self.process_store[uuid]
            if process.is_alive():
                process.terminate()

        del self.process_store[uuid]
        del self.boot_request[uuid]
        del uuid
        
        ret = self.__boot(same_uuid_br, same_uuid)

        del same_uuid_br
        del same_uuid

        return ret

    def _kill_impl(self, query:ProcessQuery) -> ProcessInstanceList:
        uuids = self._get_process_uid(query)
        ret = []
        for uuid in uuids:
            process = self.process_store[uuid]
            if process.is_alive():
                import signal
                sequence = [
                    signal.SIGINT,
                    signal.SIGKILL,
                    signal.SIGQUIT,
                ]
                for sig in sequence:
                    if not process.is_alive():
                        break
                    self.log.info(f'Sending signal \'{str(sig)}\' to \'{uuid}\'')
                    process.signal_group(sig) # TODO grab this from the inputs
                    if not process.is_alive():
                        break
                    from time import sleep
                    sleep(self.configuration.data.kill_timeout)
            pd = ProcessDescription()
            pd.CopyFrom(self.boot_request[uuid].process_description)
            pr = ProcessRestriction()
            pr.CopyFrom(self.boot_request[uuid].process_restriction)
            pu = ProcessUUID(uuid= uuid)

            return_code = None
            if not self.process_store[uuid].is_alive():
                try:
                    return_code = self.process_store[uuid].exit_code
                except Exception as e:
                    pass

            ret += [
                ProcessInstance(
                    process_description = pd,
                    process_restriction = pr,
                    status_code = ProcessInstance.StatusCode.DEAD,
                    return_code = return_code,
                    uuid = pu,

                )
            ]
            del self.process_store[uuid]

        pil = ProcessInstanceList(
            values=ret
        )
        return pil
