import getpass
import os
import signal
import tempfile
import threading
import uuid
from time import sleep

import sh
from druncschema.broadcast_pb2 import BroadcastType
from druncschema.process_manager_pb2 import (
    BootRequest,
    LogLine,
    LogRequest,
    ProcessDescription,
    ProcessInstance,
    ProcessInstanceList,
    ProcessQuery,
    ProcessRestriction,
    ProcessUUID,
)

from drunc.exceptions import DruncCommandException
from drunc.process_manager.process_manager import ProcessManager
from drunc.process_manager.utils import on_parent_exit


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
            name=self.name, session=self.session, user=self.user, exec=exc
        )


class SSHProcessManager(ProcessManager):
    def __init__(self, configuration, **kwargs):
        self.session = getpass.getuser()  # unfortunate

        super().__init__(configuration=configuration, session=self.session, **kwargs)

        # self.children_logs_depth = 1000
        # self.children_logs = {}
        self.watchers = []

        self.ssh = sh.Command("/usr/bin/ssh")

    def kill_processes(self, uuids: list) -> ProcessInstanceList:
        ret = []
        for proc_uuid in uuids:
            process = self.process_store[proc_uuid]
            app_name = self.boot_request[proc_uuid].process_description.metadata.name
            if process.is_alive():
                sequence = [
                    # signal.SIGINT, # In appfwk/daq_application, SIGQUIT makes the run marker false and quits the loop, killing the application. SIGINT not needed.
                    signal.SIGQUIT,
                    signal.SIGKILL,  # Kept as nuclear option
                ]
                for sig in sequence:
                    if not process.is_alive():
                        self.log.info(f"Killed '{app_name}' with UUID {proc_uuid}")
                        break
                    self.log.debug(
                        f"Sending signal '{str(sig).split('.')[-1]}' to '{app_name}' with UUID {proc_uuid}"
                    )
                    process.signal_group(sig)  # TODO grab this from the inputs
                    if not process.is_alive():
                        break
                    sleep(self.configuration.data.kill_timeout)
            pd = ProcessDescription()
            pd.CopyFrom(self.boot_request[proc_uuid].process_description)
            pr = ProcessRestriction()
            pr.CopyFrom(self.boot_request[proc_uuid].process_restriction)
            pu = ProcessUUID(uuid=proc_uuid)

            return_code = None
            if not self.process_store[proc_uuid].is_alive():
                try:
                    return_code = self.process_store[proc_uuid].exit_code
                except Exception:
                    pass

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

    async def _logs_impl(self, log_request: LogRequest) -> LogLine:
        self.log.debug(f"Retrieving logs for {log_request.query}")
        uid = self._ensure_one_process(self._get_process_uid(log_request.query))
        logfile = self.boot_request[uid].process_description.process_logs_path
        user = self.boot_request[uid].process_description.metadata.user
        host = self.boot_request[uid].process_description.metadata.hostname
        user_host = f"{user}@{host}"
        # https://stackoverflow.com/questions/7167008/efficiently-finding-the-last-line-in-a-text-file
        # "Not the straight forward way"...
        f = tempfile.NamedTemporaryFile(delete=False)
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
            arguments = [user_host, "-tt", "-o StrictHostKeyChecking=no"]
            arguments += ["-o LogLevel=error", "-o GlobalKnownHostsFile=/dev/null", "-o UserKnownHostsFile=/dev/null"] if host in ('localhost', '127.0.0.1', '::1') else []
            arguments += cmd

            self.ssh(
                *arguments,
                _out=f.name,
                _err_to_out=True,
            )
        except Exception as e:
            ll = LogLine(
                uuid=ProcessUUID(uuid=uid), line=f"Could not retrieve logs: {e!s}"
            )
            yield ll
            if uid in self.process_store:
                llstdout = LogLine(
                    uuid=ProcessUUID(uuid=uid),
                    line=f"stdout: {self.process_store[uid].stdout}",
                )
                llstderr = LogLine(
                    uuid=ProcessUUID(uuid=uid),
                    line=f"stderr: {self.process_store[uid].stderr}",
                )
                yield llstdout
                yield llstderr

        f.close()
        with open(f.name) as fi:
            lines = fi.readlines()
            for line in lines:
                ll = LogLine(uuid=ProcessUUID(uuid=uid), line=line)
                yield ll

        os.remove(f.name)

    def notify_join(self, name, session, user, exec):
        self.log.debug(f"{self.name} joining processes from the event loop")
        exit_code = None
        if exec:
            exit_code = exec.exit_code
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

    def __boot(self, boot_request: BootRequest, uuid: str) -> ProcessInstance:
        self.log.debug(
            f"{self.name} booting '{boot_request.process_description.metadata.name}' from session '{boot_request.process_description.metadata.session}'"
        )
        platform = os.uname().sysname.lower()
        macos = "darwin" in platform

        meta = boot_request.process_description.metadata
        if len(boot_request.process_restriction.allowed_hosts) < 1:
            raise DruncCommandException("No allowed host provided! bailing")

        error = ""

        if uuid in self.boot_request:
            raise DruncCommandException(f"Process {uuid} already exists!")
        self.boot_request[uuid] = BootRequest()
        self.boot_request[uuid].CopyFrom(boot_request)
        hostname = ""

        for host in boot_request.process_restriction.allowed_hosts:
            try:
                user = boot_request.process_description.metadata.user
                user_host = host if not user else f"{user}@{host}"
                hostname = host

                log_file = boot_request.process_description.process_logs_path
                env_var = boot_request.process_description.env

                # Add EXIT trap and use it kill child processes on the ssh client side when the ssh connection is closed
                cmd = (
                    'echo "SSHPM: Starting process $$ on host $HOSTNAME as user $USER";'
                )

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

                arguments = [
                    user_host,
                    "-tt",
                    "-o StrictHostKeyChecking=no"
                ]
                arguments += ["-o LogLevel=error", "-o GlobalKnownHostsFile=/dev/null", "-o UserKnownHostsFile=/dev/null"]  if host in ('localhost', '127.0.0.1', '::1') else []
                arguments += [
                    f"{{ {cmd} ; }} &> {log_file}",
                ]
                # arguments = [user_host, "-tt", "-o StrictHostKeyChecking=no", f'{{ {cmd} ; }} > >(tee -a {log_file}) 2> >(tee -a {log_file} >&2)']
                # I'm gonna bail now and read that log file, anyway, it's probably better that heavy logger applications don't clog up the process manager CPU.
                self.process_store[uuid] = self.ssh(
                    *arguments,
                    # _out=partial(self._process_children_logs, uuid),
                    _out=self.log.debug,
                    _err=self.log.error,
                    _bg=True,
                    _bg_exc=False,
                    _new_session=True,
                    _preexec_fn=on_parent_exit(signal.SIGTERM) if not macos else None,
                )
                self._watch(
                    name=meta.name,
                    user=meta.user,
                    session=meta.session,
                    process=self.process_store[uuid],
                )
                self.log.debug(f"Command:\nssh '{' '.join(arguments)}'")
                break

            except Exception as e:
                error += str(e)
                print(f"Couldn't start on host {host}, reason:\n{e!s}")
                print("\nTrying on a different host")
                continue
        ## Saving the host to the metadata
        self.boot_request[uuid].process_description.metadata.hostname = hostname

        self.log.info(
            f"Booted '{boot_request.process_description.metadata.name}' from session '{boot_request.process_description.metadata.session}' with UUID {uuid}"
        )
        pd = ProcessDescription()
        pd.CopyFrom(self.boot_request[uuid].process_description)
        pr = ProcessRestriction()
        pr.CopyFrom(self.boot_request[uuid].process_restriction)
        pu = ProcessUUID(uuid=uuid)

        return_code = None
        alive = False

        if uuid not in self.process_store:
            pi = ProcessInstance(
                process_description=pd,
                process_restriction=pr,
                status_code=ProcessInstance.StatusCode.DEAD,  ## should be unknown
                return_code=return_code,
                uuid=pu,
            )
            return pi

        try:
            if not self.process_store[uuid].is_alive():
                return_code = self.process_store[uuid].exit_code
            else:
                alive = True
        except Exception:
            pass

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
            if not self.process_store[proc_uuid].is_alive():
                try:
                    return_code = self.process_store[proc_uuid].exit_code
                except Exception:
                    pass

            pi = ProcessInstance(
                process_description=pd,
                process_restriction=pr,
                status_code=ProcessInstance.StatusCode.RUNNING
                if self.process_store[proc_uuid].is_alive()
                else ProcessInstance.StatusCode.DEAD,
                return_code=return_code,
                uuid=pu,
            )
            ret += [pi]

        pil = ProcessInstanceList(values=ret)

        return pil

    def _boot_impl(self, boot_request: BootRequest) -> ProcessInstance:
        self.log.debug(f"{self.name} running _boot_impl")
        this_uuid = str(uuid.uuid4())
        return self.__boot(boot_request, this_uuid)

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
            if process.is_alive():
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
