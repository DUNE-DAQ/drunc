import getpass
import uuid
from typing import Optional

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
from druncschema.request_response_pb2 import ResponseFlag

from drunc.exceptions import DruncCommandException
from drunc.process_manager.process_manager import ProcessManager
from drunc.ssh.ssh_connection_manager import SSHConnectionManager


class SSHProcessManager(ProcessManager):
    def __init__(self, configuration, **kwargs):
        self.session = getpass.getuser()  # unfortunate

        super().__init__(configuration=configuration, session=self.session, **kwargs)

        self.disable_localhost_host_key_check = False
        self.disable_host_key_check = False

        if self.configuration.data.settings:
            self.disable_localhost_host_key_check = (
                self.configuration.data.settings.get(
                    "disable_localhost_host_key_check", False
                )
            )
            self.disable_host_key_check = self.configuration.data.settings.get(
                "disable_host_key_check", False
            )

        # self.children_logs_depth = 1000
        # self.children_logs = {}

        self.ssh_manager = SSHConnectionManager(
            disable_host_key_check=self.disable_host_key_check,
            disable_localhost_host_key_check=self.disable_localhost_host_key_check,
            logger=self.log,
            on_process_exit=self._on_ssh_process_exit,
        )

    def _on_ssh_process_exit(
        self, uuid: str, exit_code: Optional[int], exception: Optional[Exception]
    ) -> None:
        """
        Callback invoked when an SSH process exits.

        Args:
            uuid: Process UUID that exited
            exit_code: Exit code from process (None if still running)
            exception: Exception if process failed abnormally
        """
        if uuid not in self.boot_request:
            return

        if exit_code is None:
            self.log.debug(
                f"Process with UUID {uuid} is still running but on_ssh_process_exit was called."
            )
        else:
            self.log.debug(f"Process with UUID {uuid} exited with code {exit_code}.")

        boot_req = self.boot_request[uuid]
        name = boot_req.process_description.metadata.name
        session = boot_req.process_description.metadata.session
        user = boot_req.process_description.metadata.user

        self.notify_join(name=name, session=session, user=user, exec=exception)

    def kill_processes(self, uuids: list) -> ProcessInstanceList:
        """
        Terminate processes by their UUIDs.

        Iterates through the provided UUID list and terminates each process
        via the SSH connection manager. Collects process status information
        for each terminated process.

        Args:
            uuids: List of process UUIDs to terminate

        Returns:
            ProcessInstanceList containing status of terminated processes
        """
        ret = []

        for proc_uuid in uuids:
            app_name = self.boot_request[proc_uuid].process_description.metadata.name

            # Terminate process if still alive
            if self.ssh_manager.is_process_alive(proc_uuid):
                self.log.debug(f"Killing '{app_name}' with UUID {proc_uuid}")
                self.ssh_manager.terminate_process(
                    proc_uuid, timeout=self.configuration.data.kill_timeout
                )
                self.log.info(f"Killed '{app_name}' with UUID {proc_uuid}")

            # Build process instance for response
            pd = ProcessDescription()
            pd.CopyFrom(self.boot_request[proc_uuid].process_description)
            pr = ProcessRestriction()
            pr.CopyFrom(self.boot_request[proc_uuid].process_restriction)
            pu = ProcessUUID(uuid=proc_uuid)

            # Get final exit code
            return_code = self.ssh_manager.get_exit_code(proc_uuid)

            ret += [
                ProcessInstance(
                    process_description=pd,
                    process_restriction=pr,
                    status_code=ProcessInstance.StatusCode.DEAD,
                    return_code=return_code,
                    uuid=pu,
                )
            ]

            # Clean up SSH resources
            self.ssh_manager.cleanup_process(proc_uuid)

        return ProcessInstanceList(
            name=self.name,
            token=None,
            values=ret,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

    def _terminate_impl(self) -> ProcessInstanceList:
        """
        Terminate all managed processes and clean up resources.

        Called during process manager shutdown to ensure all SSH connections
        and remote processes are properly terminated.

        Returns:
            ProcessInstanceList containing status of terminated processes
        """
        self.log.info("Terminating")

        if self.boot_request:
            self.log.info("Killing all the known processes before exiting")
            query = ProcessQuery(names=[".*"])
            uuids = ProcessManager._match_processes_against_query(
                query=query,
                available_uuids=list(self.ssh_manager.get_active_process_keys()),
                boot_request_dict=self.boot_request,
                order_by="leaf_first",
            )
            result = self.kill_processes(uuids)

            # Clean up all SSH manager resources
            self.ssh_manager.cleanup_all()

            return result

        self.log.info("No known process to kill before exiting")

        # Still clean up SSH manager even if no processes
        self.ssh_manager.cleanup_all()

        return ProcessInstanceList(
            name=self.name,
            token=None,
            values=[],
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

    def _logs_impl(self, log_request: LogRequest) -> LogLines:
        """
        Retrieve log output from a remote process.

        Reads the last N lines from the remote process log file via SSH connection.
        The log file location is determined from the process boot request metadata.

        Args:
            log_request: LogRequest object containing query and line count (how_far)

        Returns:
            LogLines object containing retrieved log lines or error information
        """
        self.log.debug(f"Retrieving logs for {log_request.query}")

        matching_uuids = ProcessManager._match_processes_against_query(
            query=log_request.query,
            available_uuids=list(self.ssh_manager.get_active_process_keys()),
            boot_request_dict=self.boot_request,
            order_by="random",
        )

        # Ensure exactly one process matches the query
        uid = self._ensure_one_process(matching_uuids)

        # Extract log file location and connection details from boot request
        logfile = self.boot_request[uid].process_description.process_logs_path
        user = self.boot_request[uid].process_description.metadata.user
        host = self.boot_request[uid].process_description.metadata.hostname

        # Determine number of lines to retrieve (default: 100)
        nlines = log_request.how_far if log_request.how_far else 100

        try:
            # Read log file from remote host via SSH
            lines = self.ssh_manager.read_remote_log_file(
                hostname=host, user=user, log_file=logfile, num_lines=nlines
            )

            return LogLines(
                name=self.name,
                token=None,
                uuid=ProcessUUID(uuid=uid),
                lines=lines,
                flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            )

        except Exception as e:
            # Log retrieval failed - provide error message and fallback to SSH output buffers
            lines = [f"Could not retrieve logs: {e!s}"]

            # Attempt to retrieve any captured stdout/stderr from SSH connection
            # Note: Most output is redirected to log files, so this is primarily
            # for SSH-level messages and diagnostics
            stdout = self.ssh_manager.get_process_stdout(uid)
            stderr = self.ssh_manager.get_process_stderr(uid)

            if stdout:
                lines.append(f"stdout: {stdout}")
            if stderr:
                lines.append(f"stderr: {stderr}")

            return LogLines(
                name=self.name,
                token=None,
                uuid=ProcessUUID(uuid=uid),
                lines=lines,
                flag=ResponseFlag.UNHANDLED_EXCEPTION_THROWN,
            )

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

    def __boot(self, boot_request: BootRequest, uuid: str) -> ProcessInstance:
        self.log.debug(
            f"{self.name} booting '{boot_request.process_description.metadata.name}' from session '{boot_request.process_description.metadata.session}'"
        )

        if len(boot_request.process_restriction.allowed_hosts) < 1:
            raise DruncCommandException("No allowed host provided! bailing")

        if uuid in self.boot_request:
            raise DruncCommandException(f"Process {uuid} already exists!")
        self.boot_request[uuid] = BootRequest()
        self.boot_request[uuid].CopyFrom(boot_request)
        hostname = ""

        errors = ""

        for host in boot_request.process_restriction.allowed_hosts:
            try:
                user = boot_request.process_description.metadata.user
                hostname = host
                log_file = boot_request.process_description.process_logs_path
                env_var = boot_request.process_description.env

                # Build command from executable and arguments
                cmd = ""
                for (
                    exe_arg
                ) in boot_request.process_description.executable_and_arguments:
                    cmd += exe_arg.exec
                    for arg in exe_arg.args:
                        cmd += f" {arg}"
                    cmd += ";"

                if cmd.endswith(";"):
                    cmd = cmd[:-1]

                # Execute via SSH connection manager
                self.ssh_manager.execute_ssh_command(
                    uuid=uuid,
                    boot_request=boot_request,
                    hostname=host,
                    user=user if user else getpass.getuser(),
                    command=cmd,
                    log_file=log_file,
                    env_vars=dict(env_var) if env_var else {},
                )

                self.log.debug(f"Command: {cmd}")
                break

            except Exception as e:
                errors += str(e)
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

        alive = self.ssh_manager.is_process_alive(uuid)
        return_code = self.ssh_manager.get_exit_code(uuid)
        status_code = (
            ProcessInstance.StatusCode.RUNNING
            if alive
            else ProcessInstance.StatusCode.DEAD
        )

        pi = ProcessInstance(
            process_description=pd,
            process_restriction=pr,
            status_code=status_code,
            return_code=return_code,
            uuid=pu,
        )

        return pi

    def _ps_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        """
        Retrieve process status information for processes matching the query.
        Returns process details including running status, exit codes, and metadata
        for all processes that match the provided query criteria.

        Args:
            query: ProcessQuery object containing process selection criteria

        Returns:
            ProcessInstanceList containing status information for matching processes
        """
        ret = []

        process_uuids = ProcessManager._match_processes_against_query(
            query=query,
            available_uuids=list(self.ssh_manager.get_active_process_keys()),
            boot_request_dict=self.boot_request,
            order_by="random",
        )

        # Iterate through all processes matching the query
        for proc_uuid in process_uuids:
            # Handle case where process UUID exists in boot_request but not in SSH manager
            # This can occur if process failed to start or has been cleaned up
            if proc_uuid not in self.boot_request:
                pu = ProcessUUID(uuid=proc_uuid)
                pi = ProcessInstance(
                    process_description=ProcessDescription(),
                    process_restriction=ProcessRestriction(),
                    status_code=ProcessInstance.StatusCode.DEAD,
                    return_code=None,
                    uuid=pu,
                )
                ret += [pi]
                continue

            # Copy process description and restriction from boot request
            pd = ProcessDescription()
            pd.CopyFrom(self.boot_request[proc_uuid].process_description)
            pr = ProcessRestriction()
            pr.CopyFrom(self.boot_request[proc_uuid].process_restriction)
            pu = ProcessUUID(uuid=proc_uuid)

            # Query SSH manager for process status
            alive = self.ssh_manager.is_process_alive(proc_uuid)

            return_code = (
                self.ssh_manager.get_exit_code(proc_uuid) if not alive else None
            )
            if not alive:
                self.log.debug(
                    f"Process {proc_uuid} is dead with exit code: {return_code}"
                )

            # Create process instance with current status
            pi = ProcessInstance(
                process_description=pd,
                process_restriction=pr,
                status_code=(
                    ProcessInstance.StatusCode.RUNNING
                    if alive
                    else ProcessInstance.StatusCode.DEAD
                ),
                return_code=return_code,
                uuid=pu,
            )
            ret += [pi]

        return ProcessInstanceList(
            name=self.name,
            token=None,
            values=ret,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

    def _boot_impl(self, boot_request: BootRequest) -> ProcessInstanceList:
        self.log.debug(f"{self.name} running boot command")
        this_uuid = str(uuid.uuid4())
        process = self.__boot(boot_request, this_uuid)
        return ProcessInstanceList(
            name=self.name,
            token=None,
            values=[process],
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

    def _restart_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        self.log.info(f"{self.name} restarting {query.names} in session {self.session}")

        uuids = ProcessManager._match_processes_against_query(
            query=query,
            available_uuids=list(self.boot_request.keys()),
            boot_request_dict=self.boot_request,
            order_by="random",
        )
        uuid = self._ensure_one_process(uuids, in_boot_request=True)

        same_uuid_br = BootRequest()
        same_uuid_br.CopyFrom(self.boot_request[uuid])
        same_uuid = uuid

        if self.ssh_manager.is_process_alive(uuid):
            self.ssh_manager.terminate_process(uuid)

        self.ssh_manager.cleanup_process(uuid)
        del self.boot_request[uuid]
        del uuid

        ret = [self.__boot(same_uuid_br, same_uuid)]

        del same_uuid_br
        del same_uuid

        return ProcessInstanceList(
            name=self.name,
            token=None,
            values=ret,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

    def _kill_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        """
        Kill processes matching the query.

        Terminates all processes that match the provided query criteria.

        Args:
            query: ProcessQuery object containing process selection criteria

        Returns:
            ProcessInstanceList containing status of killed processes
        """
        self.log.info(f"{self.name} killing {query.names} in session {self.session}")

        if self.boot_request:
            uuids = ProcessManager._match_processes_against_query(
                query=query,
                available_uuids=list(self.ssh_manager.get_active_process_keys()),
                boot_request_dict=self.boot_request,
                order_by="leaf_first",
            )
            return self.kill_processes(uuids)

        self.log.info("No known process to kill")
        return ProcessInstanceList(
            name=self.name,
            token=None,
            values=[],
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )
