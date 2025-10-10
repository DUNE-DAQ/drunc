import getpass
import os
import signal
import tempfile
import threading
from ctypes import CDLL
from subprocess import Popen
from time import sleep

import sh
from druncschema.broadcast_pb2 import BroadcastType
from druncschema.process_manager_pb2 import (
    BootRequest,
    LogLines,
    LogRequest,
    ProcessDescription,
    ProcessInstance,
    ProcessInstanceList,
    ProcessMetadata,
    ProcessQuery,
    ProcessRestriction,
    ProcessUUID,
)

from drunc.exceptions import DruncCommandException, DruncException
from drunc.process_manager.process_manager import (
    ProcessManager,
    ProcessManagerConfHandler,
)

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
    """
    A process manager that uses subprocess.Popen to launch and manage processes locally.
    Used for testing as a CI tool.
    """

    def __init__(self, configuration: ProcessManagerConfHandler, **kwargs):
        """
        Initialize the SubProcessProcessManager with the given configuration.

        Args:
            configuration (ProcessManagerConfHandler): The configuration handler for the
                                                        process manager.
        """
        self.session: str = getpass.getuser()  # unfortunate
        super().__init__(configuration=configuration, session=self.session, **kwargs)

        self.watchers: list[AppProcessWatcherThread] = []

    def kill_processes(self, uuids: list) -> ProcessInstanceList:
        """
        Kill the processes with the given UUIDs.

        Args:
            uuids (list): List of process UUIDs to kill.

        Returns:
            ProcessInstanceList: List of process instances that were killed.
        """

        # Make a list of the killed processes to return
        ret: list[ProcessInstance] = []

        # Iterate over the UUIDs and kill each process
        for proc_uuid in uuids:
            # Retrieve the process from the store
            process: sh.RunningCommand = self.process_store[proc_uuid]

            # Get the application name from the boot request metadata
            app_name: str = self.boot_request[
                proc_uuid
            ].process_description.metadata.name

            # Kill the process if it is still running
            if process.poll() is None:
                sequence: list[signal.Signals] = [
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

            # Construct the ProcessInstance to return
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

        return ProcessInstanceList(values=ret)

    def _terminate_impl(self) -> ProcessInstanceList:
        """
        Terminate all running processes.

        Returns:
            ProcessInstanceList: List of process instances that were terminated.
        """

        self.log.info("Terminating")

        # If there are known processes, kill them
        if self.process_store:
            self.log.info("Killing all the known processes before exiting")

            # Get all the process UUIDs
            uuids = self._get_process_uid(
                query=ProcessQuery(names=[".*"]), order_by="leaf_first"
            )
            return self.kill_processes(uuids)
        else:
            self.log.info("No known process to kill before exiting")
            return ProcessInstanceList()

    async def _logs_impl(self, log_request: LogRequest) -> LogLines:
        """
        Retrieve logs for the specified process.

        Runs the `tail` command to get the last `how_far` lines from the log file as a
        subprocess, yielding each line as a LogLines object. This is the most efficient
        way to retrieve logs without loading the entire file into memory.

        Args:
            log_request (LogRequest): The log request containing the query and how far
                                        to retrieve.

        Yields:
            LogLines: The log lines retrieved for the process.
        """

        self.log.debug(f"Retrieving logs for {log_request.query}")

        # Ensure only one process matches the query, get its log file
        uid: str = self._ensure_one_process(self._get_process_uid(log_request.query))
        logfile = self.boot_request[uid].process_description.process_logs_path

        # Use a temporary file to store the logs
        f = tempfile.NamedTemporaryFile(delete=False)
        f_file = open(f.name, "w")

        # Determine how many lines to retrieve
        nlines = log_request.how_far
        if not nlines:
            nlines = 100

        # Run the tail command to get the logs
        try:
            cmd = [
                "tail",
                f"-{nlines}",
                logfile,
            ]
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

        # Close the temporary file and read its contents
        f.close()
        with open(f.name) as fi:
            lines = fi.readlines()
            for line in lines:
                ll = LogLines(uuid=ProcessUUID(uuid=uid), line=line)
                yield ll

        # Clean up the temporary file
        os.remove(f.name)

    def notify_join(
        self, name: str, session: str, user: str, exec: sh.RunningCommand
    ) -> None:
        """
        Notify that a process has exited and perform cleanup.

        Args:
            name (str): The name of the process.
            session (str): The session associated with the process.
            user (str): The user who started the process.
            exec (sh.RunningCommand): The process that has exited.

        Returns:
            None
        """
        self.log.debug(f"{self.name} joining processes from the event loop")
        exit_code = exec.poll()

        end_msg: str = (
            f"Process '{name}' from session '{session}' with PID {exec.pid} "
            f"exited with code {exit_code}"
        )
        self.log.info(end_msg)

        if exec:
            self.log.debug(name + str(exec))

        self.broadcast(end_msg, BroadcastType.SUBPROCESS_STATUS_UPDATE)
        return

    def _watch(
        self, name: str, session: str, user: str, process: sh.RunningCommand
    ) -> None:
        """
        Start a watcher thread to monitor the given process.

        Args:
            name (str): The name of the process.
            session (str): The session associated with the process.
            user (str): The user who started the process.
            process (sh.RunningCommand): The process to watch.

        Returns:
            None
        """

        self.log.debug(f"{self.name} watching process {name}")
        t = AppProcessWatcherThread(
            pm=self, session=session, user=user, name=name, process=process
        )
        t.start()
        self.watchers.append(t)

    def __boot(self, boot_request: BootRequest) -> ProcessInstance:
        """
        Boot a new process based on the provided BootRequest.

        Args:
            boot_request (BootRequest): The request containing process description and restrictions.

        Returns:
            ProcessInstance: The instance of the booted process.
        """

        self.log.debug(
            f"{self.name} booting '{boot_request.process_description.metadata.name}' "
            f"from session '{boot_request.process_description.metadata.session}'"
        )

        # Validate the boot request
        meta: ProcessMetadata = boot_request.process_description.metadata
        if len(boot_request.process_restriction.allowed_hosts) < 1:
            raise DruncCommandException("No allowed host provided! bailing")

        error: str = ""
        pid: int | None = None
        for host in boot_request.process_restriction.allowed_hosts:
            # We can only run processes on localhost
            if host != "localhost":
                raise DruncCommandException(
                    "SubProcess process manager does not support remote hosts"
                )

            try:
                # Extract necessary information from the boot request
                hostname: str = host
                log_file: str = boot_request.process_description.process_logs_path
                env_var: dict[str, str] = boot_request.process_description.env

                # Setup the command to run
                cmd = (
                    f"SubProcessPM: Starting process {os.getpid()} on host "
                    f"{os.uname().nodename} as user {getpass.getuser()}; "
                )

                # Add exported environment variables
                cmd_env: str = ";".join(
                    [f'export {n}="{v}"' for n, v in env_var.items()]
                )
                if cmd_env:
                    cmd += cmd_env + ";"

                # Change to the specified execution directory
                exec_dir: str = (
                    boot_request.process_description.process_execution_directory
                )
                cmd += f"cd {exec_dir} ; "

                # Add the executable and its arguments
                for (
                    exe_arg
                ) in boot_request.process_description.executable_and_arguments:
                    cmd += exe_arg.exec
                    for arg in exe_arg.args:
                        cmd += f" {arg}"
                    cmd += ";"

                if cmd[-1] == ";":
                    cmd = cmd[:-1]

                # Setup the cli command to run
                arguments: str = (
                    f'drunc-process-wrapper --log {log_file} "{cmd_env}; {cmd}"'
                )
                process: Popen = Popen(
                    arguments,
                    shell=True,
                    preexec_fn=on_parent_exit(signal.SIGTERM),
                )
                self.process_store[str(process.pid)] = process
                pid: str = str(process.pid)

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

        # Add the boot request to the boot_request store
        self.boot_request[pid] = BootRequest()
        self.boot_request[pid].CopyFrom(boot_request)
        hostname: str = "localhost"
        self.boot_request[pid].process_description.metadata.hostname = hostname

        self.log.info(
            f"Booted '{boot_request.process_description.metadata.name}' from session '"
            f"{boot_request.process_description.metadata.session}' with PID {pid}"
        )

        # Construct the ProcessInstance to return
        pd = ProcessDescription()
        pd.CopyFrom(self.boot_request[pid].process_description)
        pr = ProcessRestriction()
        pr.CopyFrom(self.boot_request[pid].process_restriction)
        pu = ProcessUUID(uuid=pid)

        # If the process failed to start, return a DEAD instance
        if pid not in self.process_store:
            pi = ProcessInstance(
                process_description=pd,
                process_restriction=pr,
                status_code=ProcessInstance.StatusCode.DEAD,  ## should be unknown
                return_code=None,
                uuid=pu,
            )
            return pi

        # If the process started, return a RUNNING instance
        return_code: int | None = self.process_store[pid].poll()
        alive: bool = return_code is not None
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
        """
        List processes matching the given query.

        Args:
            query (ProcessQuery): The query to filter processes.

        Returns:
            ProcessInstanceList: List of process instances matching the query.
        """

        self.log.debug(f"{self.name} running ps")
        ret: list[ProcessInstance] = []

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
        """
        Boot a new process based on the provided BootRequest.

        Overwrites the base class method to call the internal __boot method.

        Args:
            boot_request (BootRequest): The request containing process description and
                                        restrictions.

        Returns:
            ProcessInstance: The instance of the booted process.
        """

        self.log.debug(f"{self.name} running _boot_impl")
        return self.__boot(boot_request)

    def _restart_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        """
        Restart the process matching the given query.

        Args:
            query (ProcessQuery): The query to identify the process to restart.

        Returns:
            ProcessInstanceList: List of process instances that were restarted.
        """

        self.log.info(f"{self.name} restarting {query.names} in session {self.session}")

        # Ensure only one process matches the query
        uuids: list[str] = self._get_process_uid(query, in_boot_request=True)
        uuid: str = self._ensure_one_process(uuids, in_boot_request=True)

        # Make copies of the boot request and uuid to avoid mutation issues
        same_uuid_br = BootRequest()
        same_uuid_br.CopyFrom(self.boot_request[uuid])
        same_uuid = uuid

        # Terminate the existing process if it is running
        if uuid in self.process_store:
            process = self.process_store[uuid]
            if process.poll() is None:
                process.terminate()

        # Clean up the existing process from the stores
        del self.process_store[uuid]
        del self.boot_request[uuid]
        del uuid

        # Boot a new process with the same boot request
        ret = self.__boot(same_uuid_br, same_uuid)

        # Clean up temporary copies
        del same_uuid_br
        del same_uuid

        return ret

    def _kill_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        """
        Kill the processes matching the given query.

        Args:
            query (ProcessQuery): The query to identify the processes to kill.

        Returns:
            ProcessInstanceList: List of process instances that were killed.
        """

        self.log.info(f"{self.name} killing {query.names} in session {self.session}")
        if self.process_store:
            uuids = self._get_process_uid(query, order_by="leaf_first")
            return self.kill_processes(uuids)
        else:
            self.log.info("No known process to kill before exiting")
            return ProcessInstanceList()
