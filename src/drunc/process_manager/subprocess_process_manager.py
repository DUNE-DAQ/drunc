import getpass
import os
import signal
import tempfile
import threading
from ctypes import CDLL
from subprocess import Popen
from time import sleep

from druncschema.broadcast_pb2 import BroadcastType
from druncschema.process_manager_pb2 import (
    BootRequest,
    LogLines,
    LogRequest,
    ProcessDescription,
    ProcessInstance,
    ProcessInstanceList,
    ProcessQuery,
    ProcessRestriction,
    ProcessUUID,
)

from drunc.exceptions import DruncCommandException, DruncException
from drunc.process_manager.process_manager import ProcessManager

# # ------------------------------------------------
# # pexpect.spawn(...,preexec_fn=on_parent_exit('SIGTERM'))

# Constant taken from http://linux.die.net/include/linux/prctl.h
PR_SET_PDEATHSIG = 1


class PrCtlError(DruncException):
    pass


def on_parent_exit(signum, setsid=True):
    """
    Return a function to be run in a child process which will trigger
    SIGNAME to be sent when the parent process dies
    """

    def set_parent_exit_signal():
        # http://linux.die.net/man/2/prctl
        result = CDLL("libc.so.6").prctl(PR_SET_PDEATHSIG, signum)
        if result != 0:
            raise PrCtlError("prctl failed with error code %s" % result)

        if setsid:
            os.setsid()

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
        self.process.wait()
        self.pm.notify_join(
            name=self.name, session=self.session, user=self.user, exec=self.process
        )


class SubProcessProcessManager(ProcessManager):
    def __init__(self, configuration, **kwargs):
        self.session = getpass.getuser()  # unfortunate
        super().__init__(configuration=configuration, session=self.session, **kwargs)

        self.watchers = []

    def kill_processes(self, uuids: list) -> ProcessInstanceList:
        ret = []
        for proc_uuid in uuids:
            process = self.process_store[proc_uuid]
            app_name = self.boot_request[proc_uuid].process_description.metadata.name
            if process.poll() is None:
                sequence = [
                    signal.SIGINT,
                    signal.SIGQUIT,
                    signal.SIGKILL,  # Kept as nuclear option
                ]
                for sig in sequence:
                    if process.poll() is not None:
                        self.log.info(
                            f"Process '{app_name}' already dead with PID {proc_uuid}"
                        )
                        break
                    self.log.info(
                        f"Sending signal '{str(sig).split('.')[-1]}' to '{app_name}' with UUID {proc_uuid}"
                    )
                    process.send_signal(sig)  # TODO grab this from the inputs
                    if process.poll() is not None:
                        break
                    sleep(self.configuration.data.kill_timeout)
            pd = ProcessDescription()
            pd.CopyFrom(self.boot_request[proc_uuid].process_description)
            pr = ProcessRestriction()
            pr.CopyFrom(self.boot_request[proc_uuid].process_restriction)
            pu = ProcessUUID(uuid=proc_uuid)

            return_code = self.process_store[proc_uuid].poll()

            ret += [
                ProcessInstance(
                    process_description=pd,
                    process_restriction=pr,
                    status_code=ProcessInstance.StatusCode.DEAD,
                    return_code=return_code,
                    uuid=pu,
                )
            ]
            del self.process_store[proc_uuid]

        pil = ProcessInstanceList(values=ret)
        return pil

    def _terminate_impl(self) -> ProcessInstanceList:
        self.log.info("Terminating")
        if self.process_store:
            self.log.info("Killing all the known processes before exiting")
            uuids = self._get_process_uid(
                query=ProcessQuery(names=[".*"]), order_by="leaf_first"
            )
            return self.kill_processes(uuids)
        else:
            self.log.info("No known process to kill before exiting")
            return ProcessInstanceList()

    async def _logs_impl(self, log_request: LogRequest) -> LogLines:
        self.log.debug(f"Retrieving logs for {log_request.query}")
        uid = self._ensure_one_process(self._get_process_uid(log_request.query))
        logfile = self.boot_request[uid].process_description.process_logs_path
        # https://stackoverflow.com/questions/7167008/efficiently-finding-the-last-line-in-a-text-file
        # "Not the straight forward way"...
        f = tempfile.NamedTemporaryFile(delete=False)
        f_file = open(f.name, "w")
        nlines = log_request.how_far
        if not nlines:
            nlines = 100

        try:
            cmd = [
                "tail",
                f"-{nlines}",
                logfile,
            ]
            self.log.debug(f"cmd: {cmd}")
            p = Popen(
                cmd,
                stdout=f_file,
                stderr=f_file,
            )
            p.wait()
        except Exception as e:
            ll = LogLines(
                uuid=ProcessUUID(uuid=uid), line=f"Could not retrieve logs: {e!s}"
            )
            yield ll
            if uid in self.process_store:
                llstdout = LogLines(
                    uuid=ProcessUUID(uuid=uid),
                    line=f"stdout: {self.process_store[uid].stdout}",
                )
                llstderr = LogLines(
                    uuid=ProcessUUID(uuid=uid),
                    line=f"stderr: {self.process_store[uid].stderr}",
                )
                yield llstdout
                yield llstderr

        f.close()
        with open(f.name) as fi:
            lines = fi.readlines()
            for line in lines:
                ll = LogLines(uuid=ProcessUUID(uuid=uid), line=line)
                yield ll

        os.remove(f.name)

    def notify_join(self, name, session, user, exec):
        self.log.debug(f"{self.name} joining processes from the event loop")
        exit_code = exec.poll()
        end_str = f"Process '{name}' (session: '{session}', user: '{user}') process exited with exit code {exit_code}"
        self.log.info(end_str)
        if exec:
            self.log.debug(name + str(exec))

        self.broadcast(end_str, BroadcastType.SUBPROCESS_STATUS_UPDATE)

    def _watch(self, name, session, user, process):
        self.log.debug(f"{self.name} watching process {name}")
        t = AppProcessWatcherThread(
            pm=self, session=session, user=user, name=name, process=process
        )
        t.start()
        self.watchers.append(t)

    def __boot(self, boot_request: BootRequest) -> ProcessInstance:
        self.log.debug(
            f"{self.name} booting '{boot_request.process_description.metadata.name}' from session '{boot_request.process_description.metadata.session}'"
        )

        meta = boot_request.process_description.metadata
        if len(boot_request.process_restriction.allowed_hosts) < 1:
            raise DruncCommandException("No allowed host provided! bailing")

        error = ""
        pid = None
        for host in boot_request.process_restriction.allowed_hosts:
            if host != "localhost":
                raise DruncCommandException(
                    "SubProcess process manager does not support remote hosts"
                )

            try:
                hostname = host

                log_file = boot_request.process_description.process_logs_path
                env_var = boot_request.process_description.env

                cmd = "echo SubProcessPM: Starting process $$ on host $HOSTNAME as user $USER;"

                # Add exported environment variables
                cmd_env = ";".join([f'export {n}="{v}"' for n, v in env_var.items()])
                if cmd_env:
                    cmd += cmd_env + ";"

                cmd += f"cd {boot_request.process_description.process_execution_directory} ; "

                for (
                    exe_arg
                ) in boot_request.process_description.executable_and_arguments:
                    cmd += exe_arg.exec
                    for arg in exe_arg.args:
                        cmd += f" {arg}"
                    cmd += ";"

                if cmd[-1] == ";":
                    cmd = cmd[:-1]

                # full_cmd = f"{{ {cmd} ; }} &> {log_file}"
                arguments = f'drunc-process-wrapper --log {log_file} "{cmd_env}; {cmd}"'
                # self.log.debug(f"{full_cmd}")
                process = Popen(
                    arguments,
                    shell=True,
                    preexec_fn=on_parent_exit(signal.SIGTERM),
                )
                self.process_store[str(process.pid)] = process
                pid = str(process.pid)

                self._watch(
                    name=meta.name,
                    user=meta.user,
                    session=meta.session,
                    process=self.process_store[pid],
                )
                break

            except Exception as e:
                error += str(e)
                print(f"Couldn't start on host {host}, reason:\n{e!s}")
                continue

        self.boot_request[pid] = BootRequest()
        self.boot_request[pid].CopyFrom(boot_request)
        hostname = "localhost"  # popen can only run processes on localhost
        ## Saving the host to the metadata
        self.boot_request[pid].process_description.metadata.hostname = hostname

        self.log.info(
            f"Booted '{boot_request.process_description.metadata.name}' from session '{boot_request.process_description.metadata.session}' with PID {pid}"
        )
        pd = ProcessDescription()
        pd.CopyFrom(self.boot_request[pid].process_description)
        pr = ProcessRestriction()
        pr.CopyFrom(self.boot_request[pid].process_restriction)
        pu = ProcessUUID(uuid=pid)

        return_code = None
        alive = False

        if pid not in self.process_store:
            pi = ProcessInstance(
                process_description=pd,
                process_restriction=pr,
                status_code=ProcessInstance.StatusCode.DEAD,  ## should be unknown
                return_code=return_code,
                uuid=pu,
            )
            return pi

        return_code = self.process_store[pid].poll()
        alive = return_code is not None

        pi = ProcessInstance(
            process_description=pd,
            process_restriction=pr,
            status_code=ProcessInstance.StatusCode.RUNNING
            if alive
            else ProcessInstance.StatusCode.DEAD,
            return_code=return_code,
            uuid=pu,
        )

        return pi

    def _ps_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        self.log.debug(f"{self.name} running ps")
        ret = []

        for proc_uuid in self._get_process_uid(query):
            if proc_uuid not in self.process_store:
                pu = ProcessUUID(uuid=proc_uuid)
                pi = ProcessInstance(
                    process_description=ProcessDescription(),
                    process_restriction=ProcessRestriction(),
                    status_code=ProcessInstance.StatusCode.DEAD,  # should be unknown
                    return_code=None,
                    uuid=pu,
                )
                ret += [pi]
                continue
            pd = ProcessDescription()
            pd.CopyFrom(self.boot_request[proc_uuid].process_description)
            pr = ProcessRestriction()
            pr.CopyFrom(self.boot_request[proc_uuid].process_restriction)
            pu = ProcessUUID(uuid=proc_uuid)
            return_code = None
            if self.process_store[proc_uuid].poll() is None:
                try:
                    return_code = self.process_store[proc_uuid].exit_code
                except Exception:
                    pass

            pi = ProcessInstance(
                process_description=pd,
                process_restriction=pr,
                status_code=ProcessInstance.StatusCode.RUNNING
                if self.process_store[proc_uuid].poll() is None
                else ProcessInstance.StatusCode.DEAD,
                return_code=return_code,
                uuid=pu,
            )
            ret += [pi]

        pil = ProcessInstanceList(values=ret)

        return pil

    def _boot_impl(self, boot_request: BootRequest) -> ProcessInstance:
        self.log.debug(f"{self.name} running _boot_impl")
        return self.__boot(boot_request)

    def _restart_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        self.log.info(f"{self.name} restarting {query.names} in session {self.session}")
        uuids = self._get_process_uid(query, in_boot_request=True)
        uuid = self._ensure_one_process(uuids, in_boot_request=True)

        same_uuid_br = []
        same_uuid_br = BootRequest()
        same_uuid_br.CopyFrom(self.boot_request[uuid])
        same_uuid = uuid

        if uuid in self.process_store:
            process = self.process_store[uuid]
            if process.poll() is None:
                process.terminate()

        del self.process_store[uuid]
        del self.boot_request[uuid]
        del uuid

        ret = self.__boot(same_uuid_br, same_uuid)

        del same_uuid_br
        del same_uuid

        return ret

    def _kill_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        self.log.info(f"{self.name} killing {query.names} in session {self.session}")
        if self.process_store:
            uuids = self._get_process_uid(query, order_by="leaf_first")
            return self.kill_processes(uuids)
        else:
            self.log.info("No known process to kill before exiting")
            return ProcessInstanceList()
