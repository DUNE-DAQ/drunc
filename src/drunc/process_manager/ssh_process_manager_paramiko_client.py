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
from drunc.processes.ssh_process_lifetime_manager import SSHProcessLifetimeManager


class SSHProcessManagerParamikoClient(ProcessManager):
    def __init__(self, configuration, **kwargs):
        self.ssh_lifetime_manager = None
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

        self.ssh_lifetime_manager = SSHProcessLifetimeManager(
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

        if exception is not None:
            self.log.error(
                f"Process with UUID {uuid} threw an exception when we tried to kill it: {exception!s}"
            )

        if exit_code is None:
            self.log.error(
                f"Process with UUID {uuid} is still running but on_ssh_process_exit was called."
            )
            return
        else:
            self.log.debug(f"Process with UUID {uuid} exited with code {exit_code}.")

        boot_req = self.boot_request[uuid]
        name = boot_req.process_description.metadata.name
        session = boot_req.process_description.metadata.session
        user = boot_req.process_description.metadata.user

        self.notify_join(name=name, session=session, user=user, exit_code=exit_code)

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
            if self.ssh_lifetime_manager.is_process_alive(proc_uuid):
                self.log.debug(f"Killing '{app_name}' with UUID {proc_uuid}")
                self.ssh_lifetime_manager.terminate_process(
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
            return_code = self.ssh_lifetime_manager.get_exit_code(proc_uuid)

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
            self.ssh_lifetime_manager.cleanup_process(proc_uuid)

        return ProcessInstanceList(
            name=self.name,
            token=None,
            values=ret,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

    def _get_active_process_keys(self) -> list:
        """
        Retrieve a list of active process UUIDs managed by the SSH process manager.

        Returns:
            List of active process UUID strings
        """
        return (
            list(self.ssh_lifetime_manager.get_active_process_keys())
            if self.ssh_lifetime_manager is not None
            else []
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
                available_uuids=list(self._get_active_process_keys()),
                boot_request_dict=self.boot_request,
                order_by="leaf_first",
            )
            result = self.kill_processes(uuids)

            # Clean up all SSH manager resources
            self.ssh_lifetime_manager.cleanup_all()

            return result

        self.log.info("No known process to kill before exiting")

        # Still clean up SSH manager even if no processes
        self.ssh_lifetime_manager.cleanup_all()

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
            available_uuids=list(self._get_active_process_keys()),
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
            lines = self.ssh_lifetime_manager.read_log_file(
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
            stdout = self.ssh_lifetime_manager.get_process_stdout(uid)
            stderr = self.ssh_lifetime_manager.get_process_stderr(uid)

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

    def notify_join(self, name, session, user, exit_code):
        self.log.debug(f"{self.name} sending broadcast after ssh process exit")
        end_str = f"Process '{name}' (session: '{session}', user: '{user}') process exited with exit code {exit_code}"
        self.log.info(end_str)
        self.broadcast(end_str, BroadcastType.SUBPROCESS_STATUS_UPDATE)

    def __boot(self, boot_request: BootRequest, uuid: str) -> ProcessInstance:
        """
        Boot a remote process via SSH on an available host.

        Attempts to start the process on each allowed host in sequence until
        successful. Updates boot request with the actual hostname used and
        returns process status information.

        Args:
            boot_request: BootRequest containing process configuration and restrictions
            uuid: Unique identifier for this process

        Returns:
            ProcessInstance containing process status and metadata

        Raises:
            DruncCommandException: If no allowed hosts provided or process already exists
        """
        self.log.debug(
            f"{self.name} booting '{boot_request.process_description.metadata.name}' "
            f"from session '{boot_request.process_description.metadata.session}'"
        )

        # Validate boot request
        if len(boot_request.process_restriction.allowed_hosts) < 1:
            raise DruncCommandException("No allowed host provided! bailing")

        if uuid in self.boot_request:
            raise DruncCommandException(f"Process {uuid} already exists!")

        # Store boot request for lifecycle management
        self.boot_request[uuid] = BootRequest()
        self.boot_request[uuid].CopyFrom(boot_request)

        hostname = ""
        errors = ""

        # Attempt to start process on each allowed host
        for host in boot_request.process_restriction.allowed_hosts:
            try:
                # Update hostname in boot request for this attempt
                self.boot_request[uuid].process_description.metadata.hostname = host

                # Start the process via SSH manager
                self.ssh_lifetime_manager.start_process(
                    uuid=uuid, boot_request=self.boot_request[uuid]
                )

                # Success - record the hostname used
                hostname = host
                break

            except Exception as e:
                errors += str(e)
                self.log.warning(f"Couldn't start on host {host}, reason:\n{e!s}")
                continue

        # Store the successful hostname in boot request metadata
        self.boot_request[uuid].process_description.metadata.hostname = hostname

        self.log.info(
            f"Booted '{boot_request.process_description.metadata.name}' "
            f"from session '{boot_request.process_description.metadata.session}' "
            f"with UUID {uuid}"
        )

        # Build process instance response
        pd = ProcessDescription()
        pd.CopyFrom(self.boot_request[uuid].process_description)
        pr = ProcessRestriction()
        pr.CopyFrom(self.boot_request[uuid].process_restriction)
        pu = ProcessUUID(uuid=uuid)

        # Query current process status
        alive = self.ssh_lifetime_manager.is_process_alive(uuid)
        return_code = self.ssh_lifetime_manager.get_exit_code(uuid)
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
            available_uuids=list(self._get_active_process_keys()),
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
            alive = self.ssh_lifetime_manager.is_process_alive(proc_uuid)

            return_code = (
                self.ssh_lifetime_manager.get_exit_code(proc_uuid)
                if not alive
                else None
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

        if self.ssh_lifetime_manager.is_process_alive(uuid):
            self.ssh_lifetime_manager.terminate_process(uuid)

        self.ssh_lifetime_manager.cleanup_process(uuid)
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
                available_uuids=list(self._get_active_process_keys()),
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
