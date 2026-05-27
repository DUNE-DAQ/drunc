import getpass
import threading
import uuid
from typing import List, Optional

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
from drunc.processes.exit_status import ExitStatus
from drunc.processes.ssh_process_lifetime_manager import ProcessLifetimeManager


class SSHProcessManager(ProcessManager):
    def __init__(
        self, configuration, LifetimeManagerClass: ProcessLifetimeManager, **kwargs
    ):
        # Used to prevent races between process exit callbacks and ps/kill/flush queries
        self.boot_request_lock = threading.Lock()
        self.ssh_lifetime_manager: Optional[ProcessLifetimeManager] = None
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

        self.ssh_lifetime_manager = LifetimeManagerClass(
            disable_host_key_check=self.disable_host_key_check,
            disable_localhost_host_key_check=self.disable_localhost_host_key_check,
            logger=self.log,
            on_process_exit=self._on_ssh_process_exit,
        )
        # stores the exit statuses for all dead processes by uuid
        self.archived_exit_statuses: dict[str, ExitStatus] = {}

    def _build_process_instance(
        self,
        uuid: str,
        status_code,
        return_code: int,
    ) -> ProcessInstance:
        """
        Construct a ProcessInstance from boot request data and runtime state.

        Copies process description and restriction from boot request, combines
        with current status information to create complete ProcessInstance.

        Args:
            uuid: Process UUID
            status_code: Current process status (RUNNING, DEAD, etc.)
            return_code: Process exit code if terminated, None if running

        Returns:
            Fully populated ProcessInstance object
        """
        # Copy process description from boot request
        pd = ProcessDescription()
        pd.CopyFrom(self.boot_request[uuid].process_description)

        # Copy process restriction from boot request
        pr = ProcessRestriction()
        pr.CopyFrom(self.boot_request[uuid].process_restriction)

        # Create process UUID wrapper
        pu = ProcessUUID(uuid=uuid)

        return ProcessInstance(
            process_description=pd,
            process_restriction=pr,
            status_code=status_code,
            return_code=return_code,
            uuid=pu,
            remote_pid="not available",
        )

    def _get_process_timeouts(self, uuids: List[str]) -> dict[str, float]:
        process_timeouts = {}
        for process_uuid in uuids:
            process_timeouts[process_uuid] = self.configuration.data.kill_timeout
        return process_timeouts

    def _on_ssh_process_exit(
        self,
        uuid: str,
        exit_status: Optional[ExitStatus],
        exception: Optional[Exception],
    ) -> None:
        if uuid not in self.boot_request:
            return

        if exception is not None:
            self.log.debug(
                f"Process with UUID {uuid} threw an exception when we tried to kill it: {exception!s}"
            )

        if exit_status is None:
            self.log.error(
                f"Process with UUID {uuid} is still running but on_ssh_process_exit was called."
            )
            return
        else:
            self.log.debug(
                f"Process with UUID {uuid} exited with status {exit_status!r} triggering on_ssh_process_exit."
            )

        # Processes killed cleanly via the kill endpoint will already
        # have their exit code and dead status recorded, so there is no benefit from
        # overwriting it asynchronously here. This is only used to
        # record exit codes for processes that were killed unexpectedly
        # (e.g. due to a crash or external kill signal)
        if uuid not in self.archived_exit_statuses:
            self.archived_exit_statuses[uuid] = exit_status
        if uuid not in self.expected_dead_applications:
            self.add_process_to_expected_dead_processes(uuid)

        boot_req = self.boot_request[uuid]
        name = boot_req.process_description.metadata.name
        session = boot_req.process_description.metadata.session
        user = boot_req.process_description.metadata.user

        self.notify_join(
            name=name,
            session=session,
            user=user,
            exit_status=self.archived_exit_statuses[uuid],
        )

    def kill_processes(self, uuids: list) -> ProcessInstanceList:
        """
        Kill processes by their UUIDs.

        Delegates batch termination to SSH lifetime manager. Constructs
        ProcessInstance objects from termination results.

        Args:
            uuids: List of process UUIDs to terminate

        Returns:
            ProcessInstanceList containing status of terminated processes
        """
        # Delegate shutdown to lifetime manager and retrieve exit statuses
        exit_statuses = self.ssh_lifetime_manager.kill_processes(
            uuids, self._get_process_timeouts(uuids)
        )

        for proc_uuid in uuids:
            self.add_process_to_expected_dead_processes(proc_uuid)
        for proc_uuid, exit_status in exit_statuses.items():
            if exit_status is not None:
                self.archived_exit_statuses[proc_uuid] = exit_status

        # Build ProcessInstance objects from termination results
        ret = [
            self._build_process_instance(
                uuid=uuid,
                status_code=ProcessInstance.StatusCode.DEAD,
                return_code=(
                    exit_statuses[uuid].get_reported_exit_code()
                    if exit_statuses.get(uuid) is not None
                    else None
                ),
            )
            for uuid in uuids
        ]

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

        Called during process manager shutdown. Kills processes in dependency
        order (leaf-first) if any are running.

        Returns:
            ProcessInstanceList containing status of terminated processes
        """
        self.log.info("Terminating")

        if self.boot_request:
            # Build query to match all processes
            query = ProcessQuery(names=[".*"])
            uuids = ProcessManager._match_processes_against_query(
                query=query,
                available_uuids=list(self._get_active_process_keys()),
                boot_request_dict=self.boot_request,
                order_by="leaf_first",
            )

            # Kill all matched processes
            result = self.kill_processes(uuids)

            return result

        self.log.info("No known process to kill before exiting")

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
        uid = self._ensure_one_process(matching_uuids, in_boot_request=True)

        # Extract log file location and connection details from boot request
        logfile = self.boot_request[uid].process_description.process_logs_path
        user = self.boot_request[uid].process_description.metadata.user
        host = self.boot_request[uid].process_description.metadata.hostname
        process_name = self.boot_request[uid].process_description.metadata.name

        # Determine number of lines to retrieve (default: 100)
        nlines = log_request.how_far if log_request.how_far else 100

        try:
            # Read log file from remote host via SSH
            lines = self.ssh_lifetime_manager.read_log_file(
                hostname=host, user=user, log_file=logfile, num_lines=nlines
            )

            return LogLines(
                name=process_name,
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

    def notify_join(self, name, session, user, exit_status: ExitStatus):
        self.log.debug(f"{self.name} sending broadcast after ssh process exit")
        end_str = exit_status.get_process_manager_log_message(name, session, user)
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

        # Query current process status
        alive = self.ssh_lifetime_manager.is_process_alive(uuid)
        return_status = self.ssh_lifetime_manager.pop_early_exit_status(uuid)

        # Archive exit code if process exited early
        if return_status is not None:
            self.log.debug(
                f"Process {uuid} exited early with exit status: {return_status!r}"
            )
            self.archived_exit_statuses[uuid] = return_status

        # Determine status code based on liveness
        status_code = (
            ProcessInstance.StatusCode.RUNNING
            if alive
            else ProcessInstance.StatusCode.DEAD
        )

        # Build ProcessInstance response
        pi = self._build_process_instance(
            uuid=uuid,
            status_code=status_code,
            return_code=(
                return_status.get_reported_exit_code()
                if return_status is not None
                else None
            ),
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
        with self.boot_request_lock:
            ret = []

            # Check through all processes that the lifetime manager knows about
            available_uuids = self._get_active_process_keys()

            process_uuids = ProcessManager._match_processes_against_query(
                query=query,
                available_uuids=available_uuids,
                boot_request_dict=self.boot_request,
                order_by="random",
            )

            # Iterate through all processes matching the query
            for proc_uuid in process_uuids:
                # Handle case where process UUID does not exist in the boot_request but is active in SSH manager
                # This can occur if process has been cleaned up in the process manager but is still alive in the
                # lifetime manager
                if proc_uuid not in self.boot_request:
                    pu = ProcessUUID(uuid=proc_uuid)
                    pi = ProcessInstance(
                        process_description=ProcessDescription(),
                        process_restriction=ProcessRestriction(),
                        status_code=ProcessInstance.StatusCode.DEAD,
                        return_code=None,
                        uuid=pu,
                        remote_pid="not available",
                    )
                    remote_pid_result = self.ssh_lifetime_manager.get_remote_pid(
                        proc_uuid
                    )
                    if remote_pid_result.successful:
                        pi.remote_pid = str(remote_pid_result.pid)
                    else:
                        pi.remote_pid = remote_pid_result.reason
                    ret += [pi]
                    continue

                exit_status = self.archived_exit_statuses.get(proc_uuid, None)

                if exit_status is not None:
                    pi = self._build_process_instance(
                        uuid=proc_uuid,
                        status_code=ProcessInstance.StatusCode.DEAD,
                        return_code=exit_status.get_reported_exit_code(),
                    )
                else:
                    pi = self._build_process_instance(
                        uuid=proc_uuid,
                        status_code=ProcessInstance.StatusCode.RUNNING,
                        return_code=None,
                    )

                remote_pid_result = self.ssh_lifetime_manager.get_remote_pid(proc_uuid)
                if remote_pid_result.successful:
                    pi.remote_pid = str(remote_pid_result.pid)
                else:
                    pi.remote_pid = remote_pid_result.reason
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
        # Keep track of what applications are expected to be killed so they are not
        # reported as unexpectedly dead
        self.add_process_to_expected_dead_processes(uuid)

        exit_status = self.ssh_lifetime_manager.kill_process(
            uuid, self.configuration.data.kill_timeout
        )
        if exit_status is not None:
            self.archived_exit_statuses[uuid] = exit_status

        del self.boot_request[uuid]

        ret = [self.__boot(same_uuid_br, same_uuid)]

        # Remove the application from the list of dead applications
        self.remove_process_from_expected_dead_processes(uuid)

        self.archived_exit_statuses.pop(uuid, None)
        del uuid
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
        If query.crash is True, sends SIGKILL without any cleanup to simulate
        an unexpected process crash.

        Args:
            query: ProcessQuery object containing process selection criteria.
                   Set query.crash=True to simulate a crash instead of a clean kill.

        Returns:
            ProcessInstanceList containing status of killed/crashed processes
        """
        self.log.info(f"{self.name} killing {query.names} in session {self.session}")

        if self.boot_request:
            uuids = ProcessManager._match_processes_against_query(
                query=query,
                available_uuids=list(self._get_active_process_keys()),
                boot_request_dict=self.boot_request,
                order_by="leaf_first",
            )

            if hasattr(query, "crash") and query.crash:
                return self._crash_processes(uuids)

            return self.kill_processes(uuids)

        self.log.info("No known process to kill")
        return ProcessInstanceList(
            name=self.name,
            token=None,
            values=[],
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

    def _crash_processes(self, uuids: list) -> ProcessInstanceList:
        """
        Simulate crashes for processes identified by their UUIDs.

        Sends SIGKILL to each process via the lifetime manager's crash_process
        method without performing any cleanup. This deliberately avoids marking
        processes as expected-dead so that the subsequent unexpected process
        deaths trigger crash-recovery handling.

        Args:
            uuids: List of process UUIDs to crash

        Returns:
            ProcessInstanceList containing the ProcessInstances for each
            crashed process with DEAD status and no return code.
        """
        for this_uuid in uuids:
            self.log.info(
                f"Simulating crash of process {this_uuid} (sending SIGKILL, no cleanup)."
            )
            self.ssh_lifetime_manager.crash_process(this_uuid, signal="KILL")

        ret = [
            self._build_process_instance(
                uuid=uuid,
                status_code=ProcessInstance.StatusCode.DEAD,
                return_code=None,
            )
            for uuid in uuids
        ]

        return ProcessInstanceList(
            name=self.name,
            token=None,
            values=ret,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

    def _flush_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        """Remove dead processes from tracking so they no longer appear in ps.

        Matches processes against the query, checks each for liveness via the
        SSH lifetime manager, and removes any dead ones from boot_request and
        archived_exit_statuses. Only dead processes are affected by this command.

        Args:
            query: ProcessQuery specifying which processes to consider for flushing.

        Returns:
            ProcessInstanceList containing the ProcessInstance objects that were
            successfully flushed (i.e. removed from internal tracking).
        """
        self.log.info(f"{self.name} flushing dead processes matching {query.names}")

        with self.boot_request_lock:
            candidate_uuids = ProcessManager._match_processes_against_query(
                query=query,
                available_uuids=list(self.boot_request.keys()),
                boot_request_dict=self.boot_request,
                order_by="random",
            )

        # Perform liveness checks outside the lock — these may involve SSH calls
        # and must not block the publish thread for extended periods.
        dead_uuids = []
        for proc_uuid in candidate_uuids:
            if not self.ssh_lifetime_manager.is_process_alive(proc_uuid):
                dead_uuids.append(proc_uuid)
            else:
                self.log.debug(
                    f"Process {proc_uuid} is still running — skipping flush."
                )

        flushed = []

        # Perform all mutations to boot_request under the lock so ps command always sees a
        # consistent boot_request
        with self.boot_request_lock:
            for proc_uuid in dead_uuids:
                # Guard against the process having been removed between the
                # liveness check above and acquiring the lock here.
                if proc_uuid not in self.boot_request:
                    self.log.debug(
                        f"Process {proc_uuid} was already removed before flush lock acquired — skipping."
                    )
                    continue

                exit_status = self.archived_exit_statuses.pop(proc_uuid, None)

                pi = self._build_process_instance(
                    uuid=proc_uuid,
                    status_code=ProcessInstance.StatusCode.DEAD,
                    return_code=(
                        exit_status.get_reported_exit_code()
                        if exit_status is not None
                        else None
                    ),
                )

                del self.boot_request[proc_uuid]
                # Clean data associated with the process from the lifetime manager
                self.ssh_lifetime_manager.kill_process(
                    proc_uuid, self.configuration.data.kill_timeout
                )

                self.log.info(
                    f"Flushed dead process {proc_uuid} "
                    f"(name: {pi.process_description.metadata.name}, "
                    f"exit code: {pi.return_code})."
                )
                flushed.append(pi)

        for pi in flushed:
            proc_uuid = pi.uuid.uuid
            if proc_uuid in self.expected_dead_applications:
                self.remove_process_from_expected_dead_processes(proc_uuid)

        return ProcessInstanceList(
            name=self.name,
            token=None,
            values=flushed,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )
