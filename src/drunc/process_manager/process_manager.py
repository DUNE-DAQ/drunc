import abc
import re
import threading
import time
from collections.abc import AsyncGenerator

from druncschema.authoriser_pb2 import ActionType, SystemType
from druncschema.broadcast_pb2 import BroadcastType
from druncschema.opmon.process_manager_pb2 import ProcessStatus
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
from druncschema.process_manager_pb2_grpc import ProcessManagerServicer
from druncschema.request_response_pb2 import (
    CommandDescription,
    Description,
    Response,
    ResponseFlag,
)
from google.rpc import code_pb2

from drunc.authoriser.configuration import DummyAuthoriserConfHandler
from drunc.authoriser.decorators import (
    async_authentified_and_authorised,
    authentified_and_authorised,
)
from drunc.authoriser.dummy_authoriser import DummyAuthoriser
from drunc.broadcast.server.broadcast_sender import BroadcastSender
from drunc.broadcast.server.configuration import BroadcastSenderConfHandler
from drunc.broadcast.server.decorators import async_broadcasted, broadcasted
from drunc.exceptions import DruncCommandException
from drunc.process_manager.configuration import (
    ProcessManagerConfHandler,
    ProcessManagerTypes,
)
from drunc.utils.configuration import ConfTypes
from drunc.utils.grpc_utils import (
    UnpackingError,
    pack_to_any,
    unpack_any,
    unpack_error_response,
)
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
            f"process_manager.{configuration.data.type._name_}_process_manager"
        )
        self.log.debug(pid_info_str())
        self.log.debug("Initialized ProcessManager")

        self.configuration = configuration
        self.name = name
        self.session = session

        bsch = BroadcastSenderConfHandler(
            data=self.configuration.data.broadcaster, type=ConfTypes.PyObject
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

        dach = DummyAuthoriserConfHandler(
            data=self.configuration.data.authoriser, type=ConfTypes.PyObject
        )

        self.opmon_publisher = getattr(self.configuration.data, "opmon_publisher", None)
        opmon_sleep_time = getattr(self.configuration.data, "opmon_sleep_time", 5)
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
                return_type="request_response_pb2.Description",
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
                return_type="process_manager_pb2.ProcessInstance",
            ),
            CommandDescription(
                name="boot",
                data_type=["generic_pb2.BootRequest", "None"],
                help="Start a process.",
                return_type="process_manager_pb2.ProcessInstance",
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
                help="Returns the logs from the process ( must correspond to one process). Note this is an ASYNC function",
                return_type="process_manager_pb2.LogLine",
            ),
            CommandDescription(
                name="ps",
                data_type=["process_manager_pb2.ProcessQuery"],
                help="Get the status of the listed process from the process query input (can be multiple).",
                return_type="process_manager_pb2.ProcessInstance",
            ),
        ]

        self.broadcast(message="ready", btype=BroadcastType.SERVER_READY)

        if self.opmon_publisher is not None:
            self.stop_event = threading.Event()
            self.thread = threading.Thread(
                target=self.publish,
                args=(ProcessQuery(names=[".*"]), opmon_sleep_time),
                daemon=True,
            )
            self.thread.start()

    def __del__(self):
        if self.opmon_publisher is not None:
            self.stop_event.set()
            self.thread.join()

    def publish(self, q: ProcessQuery, sleep_time: float = 5):
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
                session=self.session,
                application=self.name,
                message=ProcessStatus(
                    n_running=n_running, n_dead=n_dead, n_session=n_session
                ),
            )
            time.sleep(sleep_time)

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

    def async_interrupt_with_exception(self, *args, **kwargs):
        self.log.debug(
            f"Interrupting {self.name} broadcast asynchronously with exception"
        )
        return (
            self.broadcast_service._async_interrupt_with_exception(*args, **kwargs)
            if self.broadcast_service
            else None
        )

    @abc.abstractmethod
    def _boot_impl(self, br: BootRequest) -> ProcessInstance:
        raise NotImplementedError

    # ORDER MATTERS!
    @broadcasted  #  outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.CREATE, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def boot(self, request, context) -> Response:
        try:
            data = unpack_any(request.data, BootRequest)
        except UnpackingError as e:
            return unpack_error_response(self.__class__.__name__, str(e), request.token)

        self.log.debug(
            "{self.name} booting '{data.process_description.metadata.name}' "
            "from session '{data.process_description.metadata.session}'"
        )

        try:
            resp = self._boot_impl(data)

            return Response(
                name=self.name,
                token=None,
                data=pack_to_any(resp),
                flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
                children=[],
            )
        except NotImplementedError:
            return Response(
                name=self.name,
                token=None,
                data=pack_to_any(resp),
                flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
                children=[],
            )

    @abc.abstractmethod
    def _terminate_impl(self) -> ProcessInstanceList:
        raise NotImplementedError

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.DELETE, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def terminate(self, request, context) -> Response:
        self.log.debug(f"{self.name} terminating")
        try:
            resp = self._terminate_impl()
            return Response(
                name=self.name,
                token=None,
                data=pack_to_any(resp),
                flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
                children=[],
            )
        except NotImplementedError:
            return Response(
                name=self.name,
                token=None,
                data=pack_to_any(resp),
                flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
                children=[],
            )

    @abc.abstractmethod
    def _restart_impl(self, q: ProcessQuery) -> ProcessInstanceList:
        raise NotImplementedError

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.DELETE, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def restart(self, request, context) -> Response:
        try:
            data = unpack_any(request.data, ProcessQuery)
        except UnpackingError as e:
            return unpack_error_response(self.__class__.__name__, str(e), request.token)

        self.log.debug(f"{self.name} running restart")

        try:
            resp = self._restart_impl(data)
            return Response(
                name=self.name,
                token=None,
                data=pack_to_any(resp),
                flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
                children=[],
            )
        except NotImplementedError:
            return Response(
                name=self.name,
                token=None,
                data=pack_to_any(resp),
                flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
                children=[],
            )

    @abc.abstractmethod
    def _kill_impl(self, q: ProcessQuery) -> ProcessInstanceList:
        raise NotImplementedError

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.DELETE, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def kill(self, request, context) -> Response:
        try:
            data = unpack_any(request.data, ProcessQuery)
        except UnpackingError as e:
            return unpack_error_response(self.__class__.__name__, str(e), request.token)

        self.log.debug(f"{self.name} running kill")

        try:
            resp = self._kill_impl(data)
            return Response(
                name=self.name,
                token=None,
                data=pack_to_any(resp),
                flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
                children=[],
            )
        except NotImplementedError:
            return Response(
                name=self.name,
                token=None,
                data=pack_to_any(resp),
                flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
                children=[],
            )

    @abc.abstractmethod
    def _ps_impl(self, q: ProcessQuery) -> ProcessInstanceList:
        raise NotImplementedError

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def ps(self, request, context) -> Response:
        try:
            data = unpack_any(request.data, ProcessQuery)
        except UnpackingError as e:
            return unpack_error_response(self.__class__.__name__, str(e), request.token)

        self.log.debug(f"{self.name} running ps")

        try:
            resp = self._ps_impl(data)
            return Response(
                name=self.name,
                token=None,
                data=pack_to_any(resp),
                flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
                children=[],
            )
        except NotImplementedError:
            return Response(
                name=self.name,
                token=None,
                data=pack_to_any(resp),
                flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
                children=[],
            )

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.DELETE, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def flush(self, request, context) -> Response:
        try:
            data = unpack_any(request.data, ProcessQuery)
        except UnpackingError as e:
            return unpack_error_response(self.__class__.__name__, str(e), request.token)

        self.log.debug(f"{self.name} running flush")

        ret = []
        for uuid in self._get_process_uid(data):
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
                    status_code=ProcessInstance.StatusCode.RUNNING
                    if self.process_store[uuid].is_alive()
                    else ProcessInstance.StatusCode.DEAD,
                    return_code=return_code,
                    uuid=pu,
                )
                del self.process_store[uuid]
                ret += [pi]

        pil = ProcessInstanceList(values=ret)

        return Response(
            name=self.name,
            token=None,
            data=pack_to_any(pil),
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            children=[],
        )

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    def describe(self, request, context) -> Response:
        self.log.debug(f"{self.name} running describe")
        bd = self.describe_broadcast()
        d = Description(
            type="process_manager",
            name=self.name,
            info=self.configuration.log_path,
            session="no_session" if not self.session else self.session,
            commands=self.commands,
        )
        if bd:
            d.broadcast.CopyFrom(pack_to_any(bd))

        return Response(
            name=self.name,
            token=None,
            data=pack_to_any(d),
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            children=[],
        )

    @abc.abstractmethod
    async def _logs_impl(self, request_data: LogRequest) -> AsyncGenerator[LogLine]:
        raise NotImplementedError

    # ORDER MATTERS!
    @async_broadcasted  # outer most wrapper 1st step
    @async_authentified_and_authorised(
        action=ActionType.READ, system=SystemType.PROCESS_MANAGER
    )  # 2nd step
    async def logs(self, request, context) -> AsyncGenerator[Response]:
        """Asynchronously fetch logs for a process.

        Args:
            request: The incoming request.
            context: The gRPC context (not used).

        Yields:
            Response objects containing log lines.
        """
        self.log.debug("Getting logs")

        try:
            data = unpack_any(request.data, LogRequest)
        except UnpackingError as e:
            yield unpack_error_response(self.__class__.__name__, str(e), request.token)
            return  # Stop further processing if unpacking fails.

        try:
            async for r in await self._logs_impl(data):
                yield Response(
                    name=self.name,
                    token=None,
                    data=pack_to_any(r),
                    flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
                    children=[],
                )
        except NotImplementedError:
            yield Response(
                name=self.name,
                token=None,
                data=None,
                flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
                children=[],
            )

    def _ensure_one_process(self, uuids: [str], in_boot_request: bool = False) -> str:
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

    def _get_process_uid(
        self,
        query: ProcessQuery,
        in_boot_request: bool = False,
        order_by: str = "random",
    ) -> [str]:
        order_by = order_by.lower()
        if order_by not in ["random", "leaf_first", "root_first"]:
            raise DruncCommandException(f"Order by '{order_by}' is not supported")

        uuid_selector = []
        name_selector = query.names
        user_selector = query.user
        session_selector = query.session
        # relevent reading here: https://github.com/protocolbuffers/protobuf/blob/main/docs/field_presence.md

        for uid in query.uuids:
            uuid_selector += [uid.uuid]

        processes = []
        all_the_uuids = (
            self.process_store.keys()
            if not in_boot_request
            else self.boot_request.keys()
        )

        for uuid in all_the_uuids:
            accepted = False
            meta = self.boot_request[uuid].process_description.metadata

            if uuid in uuid_selector:
                accepted = True

            for name_reg in name_selector:
                if re.search(name_reg, meta.name):
                    accepted = True

            if session_selector == meta.session:
                accepted = True

            if user_selector == meta.user:
                accepted = True

            if accepted:
                processes.append(uuid)

        if order_by != "random":
            process_tree_position = [
                self.boot_request[x].process_description.metadata.tree_id.count(".")
                for x in processes
            ]
            processes = [x for _, x in sorted(zip(process_tree_position, processes))]
            if order_by == "leaf_first":
                processes.reverse()
        return processes

    @staticmethod
    def get(conf, **kwargs):
        log = get_logger("process_manager.get")

        if conf.data.type == ProcessManagerTypes.SSH:
            from drunc.process_manager.ssh_process_manager import SSHProcessManager

            log.info("Starting [green]SSH process_manager[/green]")
            return SSHProcessManager(conf, **kwargs)
        elif conf.data.type == ProcessManagerTypes.K8s:
            from drunc.process_manager.k8s_process_manager import K8sProcessManager

            log.info("Starting [green]K8s process_manager[/green]")
            return K8sProcessManager(conf, **kwargs)
        else:
            log.error(f"ProcessManager type {conf.get('type')} is unsupported!")
            raise RuntimeError(
                f"ProcessManager type {conf.get('type')} is unsupported!"
            )
