import abc
import re
import threading
import time

from druncschema.authoriser_pb2 import ActionType, SystemType
from druncschema.broadcast_pb2 import BroadcastType
from druncschema.description_pb2 import CommandDescription, Description
from druncschema.opmon.process_manager_pb2 import ProcessStatus
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
from druncschema.process_manager_pb2_grpc import ProcessManagerServicer
from druncschema.request_response_pb2 import (
    Request,
    ResponseFlag,
)
from google.rpc import code_pb2
from grpc import ServicerContext

from drunc.authoriser.configuration import DummyAuthoriserConfHandler
from drunc.authoriser.decorators import authentified_and_authorised
from drunc.authoriser.dummy_authoriser import DummyAuthoriser
from drunc.broadcast.server.broadcast_sender import BroadcastSender
from drunc.broadcast.server.configuration import BroadcastSenderConfHandler
from drunc.broadcast.server.decorators import broadcasted
from drunc.exceptions import DruncCommandException
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
        super().__init__()
        self.log = get_logger(
            f"process_manager.{configuration.get_data_type_name()}_process_manager"
        )
        self.log.debug(pid_info_str())
        self.log.debug("Initialized ProcessManager")

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

        self.process_store = {}  # dict[str, sh.RunningCommand]
        self.boot_request = {}  # dict[str, BootRequest]

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
        if self.opmon_publisher is not None:
            self.stop_event.set()
            self.thread.join()

    def publish(self, q: ProcessQuery, interval_s: float = 10.0):
        while not self.stop_event.is_set():
            results = self._ps_impl(q)

            n_running = sum(
                1
                for process in results.values
                if process.status_code == ProcessInstance.StatusCode.RUNNING
            )
            n_dead = sum(
                1
                for process in results.values
                if process.status_code == ProcessInstance.StatusCode.DEAD
            )
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
            return ProcessInstanceList(
                name=self.name,
                token=None,
                values=[],
                flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
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
            response = self._terminate_impl()
        except NotImplementedError:
            return ProcessInstanceList(
                name=self.name,
                token=None,
                values=[],
                flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
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
            return ProcessInstanceList(
                name=self.name,
                token=None,
                values=[],
                flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
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
            return ProcessInstanceList(
                name=self.name,
                token=None,
                values=[],
                flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
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
            return ProcessInstanceList(
                name=self.name,
                token=None,
                values=[],
                flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
            )

        return response

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.DELETE, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def flush(
        self, request: ProcessQuery, context: ServicerContext
    ) -> ProcessInstanceList:
        self.log.debug(f"{self.name} running flush")

        ret = []
        for uuid in self._get_process_uid(request):
            if uuid not in self.boot_request:
                pu = ProcessUUID(uuid=uuid)
                pi = ProcessInstance(
                    process_description=ProcessDescription(),
                    process_restriction=ProcessRestriction(),
                    status_code=ProcessInstance.StatusCode.DEAD,
                    return_code=None,
                    uuid=pu,
                )
                ret += [pi]
                continue

            pd = ProcessDescription()
            pd.CopyFrom(self.boot_request[uuid].process_description)
            pr = ProcessRestriction()
            pr.CopyFrom(self.boot_request[uuid].process_restriction)
            pu = ProcessUUID(uuid=uuid)

            return_code = None
            try:
                if not self.process_store[
                    uuid
                ].is_alive():  # OMG!! remove this implementation code
                    return_code = self.process_store[uuid].exit_code
            except Exception:
                pass

            if not self.process_store[uuid].is_alive():
                pi = ProcessInstance(
                    process_description=pd,
                    process_restriction=pr,
                    status_code=(
                        ProcessInstance.StatusCode.RUNNING
                        if self.process_store[uuid].is_alive()
                        else ProcessInstance.StatusCode.DEAD
                    ),
                    return_code=return_code,
                    uuid=pu,
                )
                del self.process_store[uuid]
                ret += [pi]

        return ProcessInstanceList(
            name=self.name,
            token=None,
            values=ret,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

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
            return LogLines(
                name=self.name,
                token=None,
                uuid=None,
                lines=[],
                flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
            )

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
            accepted = False
            meta = boot_request_dict[uuid].process_description.metadata

            # Check UUID match
            if uuid in uuid_selector:
                accepted = True

            # Check name pattern match (regex)
            for name_reg in name_selector:
                if re.search(name_reg, meta.name):
                    accepted = True

            # Check session match
            if session_selector == meta.session:
                accepted = True

            # Check user match
            if user_selector == meta.user:
                accepted = True

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
