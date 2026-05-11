import abc
import re
import sys
import threading
import time

from daqpytools.logging import LogHandlerConf, exceptions, setup_daq_ers_logger
from druncschema.authoriser_pb2 import ActionType, SystemType
from druncschema.broadcast_pb2 import BroadcastType
from druncschema.description_pb2 import CommandDescription, Description
from druncschema.opmon.process_manager_pb2 import ProcessStatus
from druncschema.process_manager_pb2 import (
    BootRequest,
    LogLines,
    LogRequest,
    ProcessInstance,
    ProcessInstanceList,
    ProcessQuery,
    GenericNotificationMessage,
    PMResponseFlag,
    PMmsg,
)
from druncschema.process_manager_pb2_grpc import ProcessManagerServicer
from druncschema.request_response_pb2 import (
    Request,
    ResponseFlag,
)

# Note: send_msg now returns PMResponseFlag (defined in process_manager.proto)
from google.rpc import code_pb2
from grpc import ServicerContext

from drunc.authoriser.configuration import DummyAuthoriserConfHandler
from drunc.authoriser.decorators import authentified_and_authorised
from drunc.authoriser.dummy_authoriser import DummyAuthoriser
from drunc.broadcast.server.broadcast_sender import BroadcastSender
from drunc.broadcast.server.configuration import BroadcastSenderConfHandler
from drunc.broadcast.server.decorators import broadcasted
from drunc.exceptions import (
    DruncCommandException,
    DruncNotImplementedException,
)
from drunc.process_manager.configuration import (
    ProcessManagerConfHandler,
    ProcessManagerTypes,
)
from drunc.utils.configuration import ConfTypes
from drunc.utils.utils import get_logger, pid_info_str


class BadQuery(DruncCommandException):
    def __init__(self, txt):
        super(BadQuery, self).__init__(txt, code_pb2.INVALID_ARGUMENT)


class ProcessManager(abc.ABC, ProcessManagerServicer):
    def __init__(
        self,
        configuration: ProcessManagerConfHandler,
        name: str,
        session: str = None,
        **kwargs,
    ):
        """C'tor. Note that this takes the ERS env variables from the
        json files defined in data/process_manager!"""
        super().__init__()

        self.log = get_logger(
            f"process_manager.{configuration.get_data_type_name()}_process_manager",
        )
        self.log.debug(pid_info_str())
        self.log.debug("Initialized ProcessManager")

        # Validate that the ERS configuration is valid
        try:
            self.handlerconf = LogHandlerConf(init_ers=True)
        except exceptions.ERSEnvError as e:
            self.log.error(
                f"Failed to set up ERS logger for process manager: [red]{e}[/red]"
            )
            sys.exit(1)

        self.ers_handler_initialized: bool = False

        self.configuration = configuration
        self.name = name
        self.session = session

        self._create_broadcast_service(self.name, self.session)

        dach = DummyAuthoriserConfHandler(
            data=self.configuration.get_data_authoriser(), type=ConfTypes.PyObject
        )

        self.opmon_publisher = getattr(
            self.configuration.get_data(), "opmon_publisher", None
        )
        interval_s = getattr(self.configuration.get_data(), "interval_s", 10.0)
        self.authoriser = DummyAuthoriser(dach, SystemType.PROCESS_MANAGER)

        self.process_store = {}  # dict[str, sh.RunningCommand] # str = uuid
        self.boot_request = {}  # dict[str, BootRequest] # str = uuid

        # Define a list of applications that we expect to die, and a lock to read the memory
        self.dead_process_lock = threading.Lock()
        self.expected_dead_applications = {}  # dict[str, BootRequest] # str == uuid

        # TODO, probably need to think of a better way to do this?
        # Maybe I should "bind" the commands to their methods, and have something looping over this list to generate the gRPC functions
        # Not particularly pretty...
        self.commands = [
            CommandDescription(
                name="describe",
                data_type=["None"],
                help="Describe self (return a list of commands, the type of endpoint, the name and session).",
                return_type="description_pb2.Description",
            ),
            CommandDescription(
                name="kill",
                data_type=["process_manager_pb2.ProcessQuery"],
                help="Kill listed process from the process query input (can be multiple).",
                return_type="process_manager_pb2.ProcessInstanceList",
            ),
            CommandDescription(
                name="restart",
                data_type=["process_manager_pb2.ProcessQuery"],
                help="Restart the process from the process query (which must correspond to one process).",
                return_type="process_manager_pb2.ProcessInstanceList",
            ),
            CommandDescription(
                name="boot",
                data_type=["generic_pb2.BootRequest", "None"],
                help="Start a process.",
                return_type="process_manager_pb2.ProcessInstanceList",
            ),
            CommandDescription(
                name="terminate",
                data_type=["process_manager_pb2.ProcessQuery"],
                help="Kill all processes in session.",
                return_type="process_manager_pb2.ProcessInstanceList",
            ),
            CommandDescription(
                name="flush",
                data_type=["process_manager_pb2.ProcessQuery"],
                help="Remove the processes from the list that are dead",
                return_type="process_manager_pb2.ProcessInstanceList",
            ),
            CommandDescription(
                name="logs",
                data_type=["process_manager_pb2.LogRequest"],
                help="Returns the logs from the process ( must correspond to one process).",
                return_type="process_manager_pb2.LogLines",
            ),
            CommandDescription(
                name="ps",
                data_type=["process_manager_pb2.ProcessQuery"],
                help="Get the status of the listed process from the process query input (can be multiple).",
                return_type="process_manager_pb2.ProcessInstanceList",
            ),
        ]

        self.broadcast(message="ready", btype=BroadcastType.SERVER_READY)

        if self.opmon_publisher is not None:
            self.stop_event = threading.Event()
            self.thread = threading.Thread(
                target=self.publish,
                args=(ProcessQuery(names=[".*"]), interval_s),
                daemon=True,
            )
            self.thread.start()

    def get_log_path(self):
        return self.configuration.get_log_path()

    def _create_broadcast_service(self, name, session):
        bsch = BroadcastSenderConfHandler(
            data=self.configuration.get_data_broadcaster(), type=ConfTypes.PyObject
        )

        self.broadcast_service = (
            BroadcastSender(
                name=name,
                session=session,
                configuration=bsch,
            )
            if bsch.data
            else None
        )

    def __del__(self):
        if hasattr(self, "opmon_publisher") and self.opmon_publisher is not None:
            self.stop_event.set()
            self.thread.join()

    def publish(self, q: ProcessQuery, interval_s: float = 10.0):
        def find_by_uuid(pi_list, target_uuid: str):
            """Identifies the process from a list by uuid"""
            for pi in pi_list.values:
                if pi.uuid.uuid == target_uuid:
                    return pi
            return None

        n_dead_prev = 0
        dead_processes_prev = set()
        while not self.stop_event.is_set():
            results = self._ps_impl(q)

            n_running = sum(
                1
                for process in results.values
                if process.status_code == ProcessInstance.StatusCode.RUNNING
            )
            dead_processes = {
                process.uuid.uuid
                for process in results.values
                if process.status_code == ProcessInstance.StatusCode.DEAD
            }
            n_dead = len(dead_processes)
            n_session = len(
                {
                    process.process_description.metadata.session
                    for process in results.values
                }
            )
            self.opmon_publisher.publish(
                message=ProcessStatus(
                    n_running=n_running, n_dead=n_dead, n_session=n_session
                ),
            )
            if n_dead_prev < n_dead:
                n_dead_prev = n_dead
                diff_set = dead_processes - dead_processes_prev
                for diff in diff_set:
                    if diff in self.expected_dead_applications:
                        self.log.debug(
                            f"Process {diff} already expected to be dead, continuing"
                        )
                        continue
                    pi = find_by_uuid(results, diff)
                    err_msg = f"Process {pi.process_description.metadata.name} has died with a return code {pi.return_code}"
                    if not self.ers_handler_initialized:
                        setup_daq_ers_logger(
                            self.log,
                            pi.process_description.metadata.session,
                            "drunc.process_manager",
                        )
                    self.log.critical(err_msg, extra=self.handlerconf.ERS)

            time.sleep(interval_s)

    """
    A couple of simple pass-through functions to the broadcasting service
    """

    def broadcast(self, *args, **kwargs):
        self.log.debug(f"{self.name} broadcasting")
        return (
            self.broadcast_service.broadcast(*args, **kwargs)
            if self.broadcast_service
            else None
        )

    def can_broadcast(self, *args, **kwargs):
        self.log.debug(f"Checking if {self.name} can broadcast")
        return (
            self.broadcast_service.can_broadcast(*args, **kwargs)
            if self.broadcast_service
            else False
        )

    def describe_broadcast(self, *args, **kwargs):
        self.log.debug(f"Describing {self.name} broadcast")
        return (
            self.broadcast_service.describe_broadcast(*args, **kwargs)
            if self.broadcast_service
            else None
        )

    def interrupt_with_exception(self, *args, **kwargs):
        self.log.debug(f"Interrupting {self.name} broadcast with exception")
        return (
            self.broadcast_service._interrupt_with_exception(*args, **kwargs)
            if self.broadcast_service
            else None
        )

    @abc.abstractmethod
    def _boot_impl(self, boot_request: BootRequest) -> ProcessInstanceList:
        raise NotImplementedError

    # ORDER MATTERS!
    @broadcasted  #  outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.CREATE, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def boot(
        self, request: BootRequest, context: ServicerContext
    ) -> ProcessInstanceList:
        self.log.debug(
            "{self.name} booting '{data.process_description.metadata.name}' "
            "from session '{data.process_description.metadata.session}'"
        )

        try:
            response = self._boot_impl(request)
        except NotImplementedError:
            raise DruncNotImplementedException(
                message="Implementation missing",
                domain="ProcessManager.boot",
            )
        except Exception as e:
            context_msg = f"Unhandled exception in ProcessManager.boot: {e}"
            self.log.exception(context_msg)

            raise DruncCommandException(
                message=context_msg,
                domain="ProcessManager.boot",
            )
        return response

    @abc.abstractmethod
    def _terminate_impl(self) -> ProcessInstanceList:
        raise NotImplementedError

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.DELETE, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def terminate(
        self, request: Request, context: ServicerContext
    ) -> ProcessInstanceList:
        self.log.debug(f"{self.name} running terminate")

        try:
            self.mark_all_processes_as_expected_dead()
            response = self._terminate_impl()
            # Remove the list of dead applications, they are expected to be dead.
            self.clear_dead_processes()
        except NotImplementedError:
            raise DruncNotImplementedException(
                message="Implementation missing",
                domain="ProcessManager.terminate",
            )
        except Exception as e:
            context_msg = f"Unhandled exception in ProcessManager.terminate: {e}"
            self.log.exception(context_msg)

            raise DruncCommandException(
                message=context_msg,
                domain="ProcessManager.terminate",
            )

        return response

    @abc.abstractmethod
    def _restart_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        raise NotImplementedError

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.DELETE, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def restart(
        self, request: ProcessQuery, context: ServicerContext
    ) -> ProcessInstanceList:
        self.log.debug(f"{self.name} running restart")

        try:
            response = self._restart_impl(request)
        except NotImplementedError:
            raise DruncNotImplementedException(
                message="Implementation missing",
                domain="ProcessManager.restart",
            )
        except Exception as e:
            context_msg = f"Unhandled exception in ProcessManager.restart: {e}"
            self.log.exception(context_msg)

            raise DruncCommandException(
                message=context_msg,
                domain="ProcessManager.restart",
            )

        return response

    @abc.abstractmethod
    def _kill_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        raise NotImplementedError

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.DELETE, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def kill(
        self, request: ProcessQuery, context: ServicerContext
    ) -> ProcessInstanceList:
        self.log.debug(f"{self.name} running kill")

        try:
            response = self._kill_impl(request)
        except NotImplementedError:
            raise DruncNotImplementedException(
                message="Implementation missing",
                domain="ProcessManager.kill",
            )
        except Exception as e:
            context_msg = f"Unhandled exception in ProcessManager.kill: {e}"
            self.log.exception(context_msg)

            raise DruncCommandException(
                message=context_msg,
                domain="ProcessManager.kill",
            )

        return response

    @abc.abstractmethod
    def _ps_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        raise NotImplementedError

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def ps(
        self, request: ProcessQuery, context: ServicerContext
    ) -> ProcessInstanceList:
        self.log.debug(f"{self.name} running ps")

        try:
            response = self._ps_impl(request)
        except NotImplementedError:
            raise DruncNotImplementedException(
                message="Implementation missing",
                domain="ProcessManager.ps",
            )
        except Exception as e:
            context_msg = f"Unhandled exception in ProcessManager.ps: {e}"
            self.log.exception(context_msg)

            raise DruncCommandException(
                message=context_msg,
                domain="ProcessManager.ps",
            )

        return response

    @abc.abstractmethod
    def _flush_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        raise NotImplementedError

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.DELETE, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def flush(
        self, request: ProcessQuery, context: ServicerContext
    ) -> ProcessInstanceList:
        """Remove dead processes from tracking so they no longer appear in ps.

        Dead processes that were killed externally (e.g. via kill -9) will remain
        visible in ps until flushed. This command clears them from internal state
        so they cannot be restarted and will not appear in subsequent ps output.

        Args:
            request: ProcessQuery specifying which processes to flush.
            context: gRPC servicer context (unused directly).

        Returns:
            ProcessInstanceList containing the processes that were flushed.
        """
        self.log.debug(f"{self.name} running flush")

        try:
            response = self._flush_impl(request)
        except NotImplementedError:
            raise DruncNotImplementedException(
                message="Implementation missing",
                domain="ProcessManager.flush",
            )
        except Exception as e:
            context_msg = f"Unhandled exception in ProcessManager.flush: {e}"
            self.log.exception(context_msg)

            raise DruncCommandException(
                message=context_msg,
                domain="ProcessManager.flush",
            )

        return response

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def describe(self, request: Request, context: ServicerContext) -> Description:
        self.log.debug(f"{self.name} running describe")

        response = Description(
            type="process_manager",
            name=self.name,
            info=self.get_log_path(),
            session="no_session" if not self.session else self.session,
            commands=self.commands,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            token=None,
        )

        if broadcast_description := self.describe_broadcast():
            response.broadcast.Pack(broadcast_description)

        return response

    @abc.abstractmethod
    def _logs_impl(self, log_request: LogRequest) -> LogLines:
        raise NotImplementedError

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def logs(self, request: LogRequest, context: ServicerContext) -> LogLines:
        """Fetch logs for a process.

        Args:
            request: The incoming request.
            context: The gRPC context (not used).

        Returns:
            A response containing log lines.
        """
        self.log.debug("Getting logs")

        try:
            response = self._logs_impl(request)
        except NotImplementedError:
            raise DruncNotImplementedException(
                message="Implementation missing",
                domain="ProcessManager.logs",
            )
        except BadQuery as e:
            return LogLines(
                name=self.name,
                token=None,
                uuid=None,
                lines=[str(e)],
                flag=ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT,
            )
        except Exception as e:
            context_msg = f"Unhandled exception in ProcessManager.logs: {e}"
            self.log.exception(context_msg)

            raise DruncCommandException(
                message=f"{context_msg}: {e}",
                domain="ProcessManager.logs",
            )

        return response

    @abc.abstractmethod
    def _send_msg_impl(self, msg: str | None = None) -> PMmsg:
        raise NotImplementedError

    @broadcasted
    @authentified_and_authorised(
        action=ActionType.READ, system=SystemType.PROCESS_MANAGER
    )
    def send_msg(self, request: Request, context: ServicerContext) -> PMmsg:
        self.log.debug(f"{self.name} running send_msg")
        # Try to extract an optional GenericNotificationMessage from request.data
        msg_value = None
        try:
            if (
                request is not None
                and hasattr(request, "data")
                and request.data is not None
            ):
                gm = GenericNotificationMessage()
                try:
                    request.data.Unpack(gm)
                    msg_value = gm.message
                except Exception:
                    # If unpacking fails, ignore and proceed with None
                    msg_value = None

        except Exception as e:
            self.log.debug(
                f"Error while extracting send_msg payload: {e}", exc_info=True
            )

        try:
            response = self._send_msg_impl(msg_value)
        except NotImplementedError:
            raise DruncNotImplementedException(
                message="Implementation missing",
                domain="ProcessManager.send_msg",
            )
        except Exception as e:
            context_msg = f"Unhandled exception in ProcessManager.send_msg: {e}"
            self.log.exception(context_msg)

            raise DruncCommandException(
                message=context_msg,
                domain="ProcessManager.send_msg",
            )

        # Expect a PMResponseFlag enum instance
        return response

    def _ensure_one_process(
        self, uuids: list[str], in_boot_request: bool = False
    ) -> str:
        if uuids == []:
            raise BadQuery("The process corresponding to the query doesn't exist")
        elif len(uuids) > 1:
            raise BadQuery("There are more than 1 processes corresponding to the query")

        if in_boot_request:
            if uuids[0] not in self.boot_request:
                raise BadQuery(
                    f"Couldn't find the process corresponding to the UUID {uuids[0]} in the boot requests"
                )
        else:
            if uuids[0] not in self.process_store:
                raise BadQuery(
                    f"Couldn't find the process corresponding to the UUID {uuids[0]} in the process store"
                )
        return uuids[0]

    @staticmethod
    def _match_processes_against_query(
        query: ProcessQuery,
        available_uuids: list[str],
        boot_request_dict: dict,
        order_by: str = "random",
    ) -> list[str]:
        """
        Static method to match process UUIDs against query criteria.

        Filters the provided UUIDs based on query parameters and returns matching
        processes in the specified order. This method is stateless and can be used
        by any process manager implementation.

        Args:
            query: ProcessQuery containing selection criteria (names, users, sessions, UUIDs)
            available_uuids: List of process UUIDs to search through
            boot_request_dict: Dictionary mapping UUIDs to boot requests (must contain process_description.metadata)
            order_by: Sort order - "random", "leaf_first", or "root_first"

        Returns:
            List of process UUIDs matching the query criteria

        Raises:
            DruncCommandException: If order_by parameter is invalid
        """
        order_by = order_by.lower()
        if order_by not in ["random", "leaf_first", "root_first"]:
            raise DruncCommandException(f"Order by '{order_by}' is not supported")

        # Extract query selectors
        uuid_selector = [uid.uuid for uid in query.uuids]
        name_selector = query.names
        user_selector = query.user
        session_selector = query.session
        # relevent reading here: https://github.com/protocolbuffers/protobuf/blob/main/docs/field_presence.md

        # Filter processes based on query criteria
        processes = []
        for uuid in available_uuids:
            accepted = True
            meta = boot_request_dict[uuid].process_description.metadata

            # Check UUID match
            if uuid_selector and uuid not in uuid_selector:
                accepted = False

            # Check name pattern match (regex)

            if name_selector and not any(
                re.search(reg, meta.name) for reg in name_selector
            ):
                accepted = False

            # Check session match
            if session_selector and session_selector != meta.session:
                accepted = False

            # Check user match
            if user_selector and user_selector != meta.user:
                accepted = False

            if accepted:
                processes.append(uuid)

        # Apply ordering if requested
        if order_by != "random":
            # Sort by tree depth (number of dots in tree_id)
            process_tree_position = [
                boot_request_dict[x].process_description.metadata.tree_id.count(".")
                for x in processes
            ]
            processes = [x for _, x in sorted(zip(process_tree_position, processes))]

            # Reverse for leaf-first ordering
            if order_by == "leaf_first":
                processes.reverse()

        return processes

    def add_process_to_expected_dead_processes(self, uuid: str) -> None:
        """
        Add the process to the list of processes that are expected to die. Needed as the
        OpMon publisher publishes the state when a process dies unexpectedly, and these
        processes require tracking.

        Args:
            uuid: str - process UUId to add to the dict

        Returns:
            None

        Raises:
            DruncException - if the process is not known about, this error gets raised
        """
        with self.dead_process_lock:
            if uuid in self.boot_request:
                br = BootRequest()
                br.CopyFrom(self.boot_request[uuid])
                self.expected_dead_applications[uuid] = br
            else:
                err_msg = f"Unexpected process with UUID {uuid} requested to be added to the list of dead applications!"
                self.log.error(err_msg)

    def remove_process_from_expected_dead_processes(self, uuid: str) -> None:
        """
        Remove the process to the list of processes that are expected to die. Needed as
        the OpMon publisher publishes the state when a process dies unexpectedly, and
        these processes require tracking.

        Args:
            uuid: str - process UUId to add to the dict

        Returns:
            None

        Raises:
            DruncException - if the process is not known about, this error gets raised
        """
        with self.dead_process_lock:
            if uuid in self.expected_dead_applications:
                self.expected_dead_applications.pop(uuid, None)
            else:
                err_msg = f"Unexpected process with UUID {uuid} requested to be removed from the list of expected_dead_applications!"
                self.log.error(err_msg)

    def mark_all_processes_as_expected_dead(self) -> None:
        """
        Remove all processes from the tracker of expected dead processes

        Args:
            None

        Returns:
            None

        Raises:
            None
        """
        with self.dead_process_lock:
            for proc_uuid in self.boot_request:
                if proc_uuid in self.expected_dead_applications:
                    continue
                self.expected_dead_applications[proc_uuid] = self.boot_request[
                    proc_uuid
                ]

    def clear_dead_processes(self) -> None:
        """
        Remove all processes from the tracker of expected dead processes

        Args:
            None

        Returns:
            None

        Raises:
            None
        """
        with self.dead_process_lock:
            self.expected_dead_applications.clear()

    def _get_process_uid(
        self,
        query: ProcessQuery,
        in_boot_request: bool = False,
        order_by: str = "random",
    ) -> list[str]:
        """
        Find process UUIDs matching the query criteria.

        Searches through registered processes and returns UUIDs that match
        the specified query parameters (names, users, sessions, UUIDs).

        Args:
            query: ProcessQuery containing selection criteria
            in_boot_request: If True, search boot_request keys; if False, search process_store keys
            order_by: Sort order - "random", "leaf_first", or "root_first"

        Returns:
            List of process UUIDs matching the query criteria
        """
        # Determine which UUID collection to search
        all_the_uuids = (
            list(self.process_store.keys())
            if not in_boot_request
            else list(self.boot_request.keys())
        )

        # Use static method to perform matching
        matched_processes = self._match_processes_against_query(
            query=query,
            available_uuids=all_the_uuids,
            boot_request_dict=self.boot_request,
            order_by=order_by,
        )
        return matched_processes

    @staticmethod
    def get(conf, **kwargs):
        log = get_logger("process_manager.get")

        if conf.data.type == ProcessManagerTypes.SSH_SHELL:
            from drunc.process_manager.ssh_process_manager_shell import (
                SSHProcessManagerShell,
            )

            log.debug("Starting [green]SSH Shell process_manager[/green]")
            return SSHProcessManagerShell(conf, **kwargs)
        elif conf.data.type == ProcessManagerTypes.K8s:
            from drunc.process_manager.k8s_process_manager import K8sProcessManager

            log.debug("Starting [green]K8s process_manager[/green]")
            return K8sProcessManager(conf, **kwargs)
        elif conf.data.type == ProcessManagerTypes.SSH_PARAMIKO:
            from drunc.process_manager.ssh_process_manager_paramiko_client import (
                SSHProcessManagerParamikoClient,
            )

            log.debug("Starting [green]SSH Paramiko process_manager[/green]")
            return SSHProcessManagerParamikoClient(conf, **kwargs)
        else:
            log.error(f"ProcessManager type {conf.get('type')} is unsupported!")
            raise RuntimeError(
                f"ProcessManager type {conf.get('type')} is unsupported!"
            )
