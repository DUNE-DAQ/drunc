import multiprocessing
import re
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from typing import Callable, List, TypeVar

from druncschema.authoriser_pb2 import ActionType, SystemType
from druncschema.broadcast_pb2 import BroadcastType
from druncschema.controller_pb2 import (
    AddressedCommand,
    DescribeFSMResponse,
    DescribeResponse,
    ExecuteFSMCommandRequest,
    ExecuteFSMCommandResponse,
    FSMResponseFlag,
    RecomputeStatusResponse,
    StatusResponse,
)
from druncschema.controller_pb2_grpc import ControllerServicer
from druncschema.description_pb2 import Description
from druncschema.generic_pb2 import PlainText, Stacktrace
from druncschema.opmon.generic_pb2 import RunInfo
from druncschema.request_response_pb2 import Response, ResponseFlag
from druncschema.token_pb2 import Token
from google.protobuf.any_pb2 import Any
from grpc import ServicerContext

from drunc.authoriser.configuration import DummyAuthoriserConfHandler
from drunc.authoriser.decorators import authentified_and_authorised
from drunc.authoriser.dummy_authoriser import DummyAuthoriser
from drunc.broadcast.server.broadcast_sender import BroadcastSender
from drunc.broadcast.server.configuration import BroadcastSenderConfHandler
from drunc.broadcast.server.decorators import broadcasted
from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.controller.children_interface.child_node import ChildNode
from drunc.controller.children_interface.rest_api_child import ResponseListener
from drunc.controller.controller_actor import ControllerActor
from drunc.controller.decorators import in_control, publish_command_time
from drunc.controller.stateful_node import CannotExclude, CannotInclude, StatefulNode
from drunc.controller.utils import (
    ControllerMonitoringMetrics,
    get_detector_name,
    get_status_message,
)
from drunc.exceptions import DruncCommandException, DruncException
from drunc.fsm.actions.utils import get_dotdrunc_json
from drunc.fsm.configuration import FSMConfHandler
from drunc.fsm.exceptions import (
    DotDruncJsonIncorrectFormat,
    DotDruncJsonNotFound,
)
from drunc.fsm.utils import convert_fsm_transition
from drunc.utils.grpc_utils import UnpackingError, pack_to_any, unpack_any
from drunc.utils.utils import get_logger

T = TypeVar("T")


def OLD_address_command(
    obj,
    command_name,
    command_data,
    target,
    execute_along_path,
    execute_on_all_subsequent_children_in_path,
):
    log = get_logger("controller.OLD_address_command")

    ret = {}
    children_names = [c.name for c in obj.children_nodes]

    start_with_slash = target.startswith("/")
    target_ = target[:]
    if start_with_slash:
        target_ = target[1:]

    if target_ == "":
        if execute_on_all_subsequent_children_in_path:
            for child in children_names:
                ret[child] = AddressedCommand(
                    command_name=command_name,
                    command_data=command_data,
                    target=child,
                    execute_along_path=execute_along_path,
                    execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
                )
        return ret

    target_path = target_.split("/")
    if start_with_slash and target_path[0] != obj.name:
        raise DruncCommandException(f"Target '{target_}' is not matching '{obj.name}'")

    if target_path[0] == obj.name:
        target_path.pop(0)

    if target_path == []:
        if execute_on_all_subsequent_children_in_path:
            for child in children_names:
                ret[child] = AddressedCommand(
                    command_name=command_name,
                    command_data=command_data,
                    target=child,
                    execute_along_path=execute_along_path,
                    execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
                )
        return ret

    target_name = target_path[0]

    for child in children_names:
        if re.match(target_name, child):
            new_target_path = child
            if len(target_path) > 1:
                new_target_path = "/".join([new_target_path] + target_path[1:])
            ret[child] = AddressedCommand(
                command_name=command_name,
                command_data=command_data,
                target=new_target_path,
                execute_along_path=execute_along_path,
                execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
            )

    if ret == {}:
        log.info(f"Target '{target}' not found in children of '{obj.name}'")

    return ret


def OLD_unpack_addressed_command_to(data_type=None):
    def decor(cmd):
        command_name = cmd.__name__
        logger = get_logger(f"controller.upack_add'ed_cmd.{command_name}")

        @wraps(cmd)
        def wrap(obj, request, context):
            try:
                command = unpack_any(request.data, AddressedCommand)
            except UnpackingError as e:
                logger.exception(e)
                return Response(
                    name=obj.name,
                    token=None,
                    data=pack_to_any(PlainText(text=str(e))),
                    flag=ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT,
                    children=[],
                )

            try:
                addressed_commands = OLD_address_command(
                    obj=obj,
                    command_name=command_name,
                    command_data=command.command_data,
                    target=command.target,
                    execute_along_path=command.execute_along_path,
                    execute_on_all_subsequent_children_in_path=command.execute_on_all_subsequent_children_in_path,
                )
                logger.debug(f"Addressed commands: {addressed_commands}")
            except DruncCommandException as e:
                logger.exception(e)
                return Response(
                    name=obj.name,
                    token=None,
                    data=pack_to_any(PlainText(text=str(e))),
                    flag=ResponseFlag.FAILED,
                    children=[],
                )

            payload = None
            if data_type is not None:
                try:
                    payload = unpack_any(command.command_data, data_type)
                except UnpackingError as e:
                    logger.exception(e)
                    return Response(
                        name=obj.name,
                        token=None,
                        data=pack_to_any(PlainText(text=str(e))),
                        flag=ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT,
                        children=[],
                    )

            execute_on_self = (
                command.target == obj.name
                or command.target == ""
                or command.target == "/"
                or command.execute_along_path
            )

            kwargs = {
                "addressed_commands": addressed_commands,
                "execute_on_self": execute_on_self,
                "token": request.token,
            }
            if payload is not None:
                kwargs["payload"] = payload

            return cmd(obj, **kwargs)

        return wrap

    return decor


class Controller(ControllerServicer):
    children_nodes: List[ChildNode] = []

    def __init__(self, configuration, name: str, session: str, token: Token):
        super().__init__()

        self.name = name
        self.session = session
        self.broadcast_service = None
        self.monitoring_metrics = ControllerMonitoringMetrics()

        self.log = get_logger("controller")
        log_init = get_logger("controller.__init__")
        log_init.info(f"Initialising controller '{name}' with session '{session}'")

        self.configuration = configuration
        self.top_segment_controller = (
            self.configuration.db.get_dal(
                class_name="Session", uid=self.configuration.oks_key.session
            ).segment.controller.id
            == self.name
        )
        self.custom_origin = {"top_segment_controller": self.top_segment_controller}

        self.runinfo = {}
        self.runinfo["Configuration"] = self.configuration.initial_data.removeprefix(
            "oksconflibs:"
        )
        self.opmon_publisher = getattr(self.configuration, "opmon_publisher", None)
        bsch = BroadcastSenderConfHandler(
            data=self.configuration.data.controller.broadcaster,
        )

        self.broadcast_service = BroadcastSender(
            name=name,
            session=session,
            configuration=bsch,
        )

        self.fsm_config = FSMConfHandler(
            data=self.configuration.data.controller.fsm,
        )

        self.stateful_node = StatefulNode(
            fsm_configuration=self.fsm_config,
            publisher=self.controller_publisher,
            init_state="initialising",
            name=name,
            session=session,
            top_segment_controller=self.top_segment_controller,
        )

        dach = DummyAuthoriserConfHandler(
            data=self.configuration.authoriser,
        )

        self.authoriser = DummyAuthoriser(dach, SystemType.CONTROLLER)

        self.actor = ControllerActor(token)

        self.connectivity_service = None
        self.connectivity_service_thread = None
        self.uri = ""
        if self.configuration.session.connectivity_service:
            connection_server = self.configuration.session.connectivity_service.host
            connection_port = (
                self.configuration.session.connectivity_service.service.port
            )
            log_init.info(
                f"Connectivity server {connection_server}:{connection_port} is enabled"
            )

            self.connectivity_service = ConnectivityServiceClient(
                session=self.session,
                address=f"{connection_server}:{connection_port}",
            )

        self.children_nodes = self.configuration.get_dummy_children()

    def init_controller(self) -> None:
        log_init_controller = get_logger("controller.init_controller")
        log_init_controller.info("Finishing initialisation of controller")
        self.configuration.update_children(
            self.children_nodes,
            init_token=self.actor.get_token(),
            connectivity_service=self.connectivity_service,
            session_name=self.session,
        )
        # At this point, we already waited for 60s for the children applications to
        # start and show up on the connectivity service
        # We now wait for each application to get from "initialising" to "ready"
        # Unfortunately, if an application crashed on boot and never made it to the
        # connectivity service,
        # its parent controller will only notice it after 60s, so we need to wait for a
        # _bit more_ than 60s for that controller to come out of initialising state.
        # Let's assume that parent controller takes 10s to get from initialising to
        # ready, in error state.
        timeout = 60 + 10

        time_start = time.time()

        while (
            time.time() - time_start < timeout
            and self.stateful_node.node_is_in_error() == False
        ):

            def child_command(child: ChildNode, target: str) -> StatusResponse:
                return child.status(target)

            child_list = self.address_all()
            child_responses = self.propagate_concurrently(child_command, child_list)

            children_states = {}
            for response in child_responses:
                children_states[response.name] = response.status.state
                if response.status.in_error:
                    self.stateful_node.to_error()

            if any([c.lower() != "initial" for c in children_states.values()]):
                time.sleep(0.5)
            else:
                break

        bad_children = [k for k, v in children_states.items() if v.lower() != "initial"]
        if bad_children:
            log_init_controller.error(
                f"Children that did not initialise in time: {bad_children}"
            )
            self.stateful_node.to_error()

        for child in self.children_nodes:
            if child.name in bad_children:
                continue
            log_init_controller.info(f"Taking control of {child.name}")
            request = AddressedCommand(
                token=self.actor.get_token(),
                command_name="take_control",
                command_data=None,
                target=child.name,
                execute_along_path=True,
                execute_on_all_subsequent_children_in_path=True,
            )
            child.propagate_command("take_control", request, self.actor.get_token())

        interval_s = getattr(self.configuration.data, "interval_s", 10.0)

        if self.opmon_publisher is not None:
            self.stop_event = threading.Event()
            self.thread = threading.Thread(
                target=self.threading_publish_state,
                args=(interval_s,),
                daemon=True,
            )
            self.thread.start()

        self.broadcast(message="ready", btype=BroadcastType.SERVER_READY)
        self.stateful_node.set_ready_state(True)
        log_init_controller.info("Controller ready")

    """
    A couple of simple pass-through functions to the broadcasting service
    """

    def broadcast(self, *args, **kwargs):
        return self.broadcast_service.broadcast(*args, **kwargs)

    def can_broadcast(self, *args, **kwargs):
        if self.broadcast_service:
            return self.broadcast_service.can_broadcast(*args, **kwargs)
        return False

    def describe_broadcast(self, *args, **kwargs):
        return self.broadcast_service.describe_broadcast(*args, **kwargs)

    def interrupt_with_exception(self, *args, **kwargs):
        return self.broadcast_service._interrupt_with_exception(*args, **kwargs)

    def controller_publisher(self, message, custom_origin: dict | None = None):
        if self.opmon_publisher is not None:
            try:
                if custom_origin is None:
                    custom_origin = {}

                self.opmon_publisher.publish(
                    message=message,
                    custom_origin=custom_origin | self.custom_origin,
                )
                self.log.debug(f"Published {type(message)} to OpMon")
            except Exception as e:
                self.log.error(f"Failed to publish to OpMon: {e}")

    def threading_publish_state(self, interval_s: float = 10.0):
        while not self.stop_event.is_set():
            try:
                self.stateful_node.publish_state()
                current_state = self.stateful_node.get_node_operational_state()
                self.log.debug(
                    f"Publishing periodic FSM status: {current_state} every {interval_s}s"
                )

                if self.runinfo and self.runinfo.get("run", None) is not None:
                    self.monitoring_metrics.run_type = self.runinfo.get(
                        "production_vs_test", ""
                    )
                    self.monitoring_metrics.run_number = self.runinfo.get("run", 0)
                    self.monitoring_metrics.disable_data_storage = self.runinfo.get(
                        "disable_data_storage", False
                    )
                    self.monitoring_metrics.trigger_rate = self.runinfo.get(
                        "trigger_rate", 0.0
                    )
                    self.monitoring_metrics.run_time_at_start = self.runinfo.get(
                        "run_time_at_start", 0
                    )

                if current_state not in ("none", "initial", "configured"):
                    if self.monitoring_metrics.run_time_at_start:
                        self.monitoring_metrics.run_time_since_start = int(
                            time.time() - self.monitoring_metrics.run_time_at_start
                        )

                self.log.debug(f"Publishing periodic run info every {interval_s}s")
                self.controller_publisher(
                    message=RunInfo(
                        run_type=self.monitoring_metrics.run_type,
                        trigger_rate=self.monitoring_metrics.trigger_rate,
                        run_number=self.monitoring_metrics.run_number,
                        disable_data_storage=self.monitoring_metrics.disable_data_storage,
                        run_time_at_start=int(
                            self.monitoring_metrics.run_time_at_start
                        ),
                        run_time_since_start=self.monitoring_metrics.run_time_since_start,
                        run_config_file=self.configuration.oks_path,
                        run_config_name=self.configuration.oks_key.session,
                    ),
                    # custom_origin = self.controller.custom_origin
                )
            except Exception as e:
                self.log.exception(f"Error while publishing periodic status: {e}")
            time.sleep(interval_s)

    def advertise_control_address(self, address):
        self.uri = address

        if not self.connectivity_service:
            return

        self.log.info(
            f"Registering {self.name} ({address}) to the connectivity service at {self.connectivity_service.address}"
        )

        self.running = True

        def update_connectivity_service(ctrler, connectivity_service, interval):
            while ctrler.running:
                ctrler.connectivity_service.publish(
                    ctrler.name + "_control",
                    ctrler.uri,
                    "RunControlMessage",
                )
                time.sleep(interval)

        self.connectivity_service_thread = threading.Thread(
            target=update_connectivity_service,
            args=(self, self.connectivity_service, 2),
            name="connectivity_service_updating_thread",
        )

        # lets roll
        self.connectivity_service_thread.start()

    def terminate(self):
        self.running = False
        if self.opmon_publisher is not None:
            self.stop_event.set()
            self.thread.join()

        if hasattr(self, "connectivity_service") and self.connectivity_service:
            if self.connectivity_service_thread:
                self.connectivity_service_thread.join()
            self.log.info("Unregistering from the connectivity service")
            self.connectivity_service.retract(self.name + "_control")

        if self.can_broadcast():
            self.broadcast(
                btype=BroadcastType.SERVER_SHUTDOWN,
                message="over_and_out",
            )

        self.log.info("Stopping children")
        for child in self.children_nodes:
            self.log.debug(f"Stopping {child.name}")
            child.terminate()
        self.children_nodes = []

        if ResponseListener.exists():
            ResponseListener.get().terminate()

        self.log.debug("Threading threads")
        for t in threading.enumerate():
            self.log.debug(f"{t.name} TID: {t.native_id} is_alive: {t.is_alive}")

        with multiprocessing.Manager() as manager:
            self.log.debug("Multiprocess threads")
            self.log.debug(manager.list())

    def __del__(self):
        self.terminate()

    def OLD_propagate_to_all_children(
        self,
        command_name: str,
        token: Token,
        command_data: Any = None,
        only_included: bool = True,
    ):
        children_to_execute = [
            cn.name for cn in self.children_nodes if not only_included or cn.included
        ]

        addressed_commands = {
            cn: AddressedCommand(
                command_name=command_name,
                command_data=command_data,
                target=cn,
                execute_along_path=True,
                execute_on_all_subsequent_children_in_path=True,
            )
            for cn in children_to_execute
        }

        return self.OLD_propagate_to_children(
            command_name,
            addressed_commands,
            token,
        )

    def OLD_propagate_to_children(
        self,
        command_name: str,
        addressed_commands: dict[str, AddressedCommand],
        token: Token,
    ):
        self.log.debug(f"Propagating {command_name} to children")
        response_children: list[Response] = []
        response_lock = threading.Lock()

        def propagate_to_child(
            child_name,
            command_name,
            command_data,
            token,
            response_lock,
            response_children,
        ):
            child = next(
                (cn for cn in self.children_nodes if cn.name == child_name), None
            )

            if child is None:
                self.log.error(f"Child {child_name} not found")
                return

            command_data_str = str(command_data).replace("\n", " ")
            self.log.debug(
                f"Propagating {command_name} to child {child.name}, command data: {command_data_str}, token: {token}"
            )

            try:
                response = child.propagate_command(command_name, command_data, None)
                with response_lock:
                    response_children.append(response)

                if response.flag in [
                    ResponseFlag.EXECUTED_SUCCESSFULLY,
                    ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
                ]:
                    self.log.debug(
                        f"Propagated {command_name} to children ({child.name}) successfully"
                    )
                else:
                    self.log.error(
                        f"Propagating {command_name} to children ({child.name}) failed: {ResponseFlag.Name(response.flag)}. See its logs for more information and stacktrace."
                    )

            except Exception as e:  # Catch all, we are in a thread and want to do something sensible when an exception is thrown
                self.log.error(
                    f"Something wrong happened while sending the command to {child.name}: Error raised: {e!s}"
                )
                self.log.exception(e)
                flag = (
                    ResponseFlag.DRUNC_EXCEPTION_THROWN
                    if isinstance(e, DruncException)
                    else ResponseFlag.UNHANDLED_EXCEPTION_THROWN
                )

                with response_lock:
                    stack = traceback.format_exc().split("\n")
                    response_children.append(
                        Response(
                            name=child.name,
                            token=token,
                            data=pack_to_any(Stacktrace(text=stack)),
                            flag=flag,
                            children=[],
                        )
                    )

                self.log.error(
                    f"Failed to propagate {command_name} to {child.name} ({child.name}) EXCEPTION THROWN: {str(e)}"
                )

        threads = []

        for child, data in addressed_commands.items():
            self.log.debug(f"Propagating to {child}")
            t = threading.Thread(
                target=propagate_to_child,
                kwargs={
                    "child_name": child,
                    "command_name": command_name,
                    "command_data": data,
                    "token": token,
                    "response_lock": response_lock,
                    "response_children": response_children,
                },
            )
            t.start()
            threads.append(t)

        for thread in threads:
            thread.join()

        return response_children

    def parse_target_string(self, target: str) -> str:
        """Parse and check a target string.

        1. Set it to the current node name if it is empty.
        2. Ensure that it starts with the current node name.

        Args:
            target: The path to the target, as a raw string.

        Returns:
            The (possibly modified) target string.

        Raises:
            ValueError: If it does not start at the current node.
        """
        if not target:
            return self.name

        target_path = target.split("/")
        if target_path[0] != self.name:
            error_str = f"Target '{target}' does not start with '{self.name}'"
            self.log.error(error_str)
            raise ValueError(error_str)

        return target

    def address_target_path(
        self,
        target: str,
        execute_on_children: bool,
        ignore_exclusion: bool = False,
    ) -> list[tuple[ChildNode, str]]:
        """Finds the next node(s) along a given path to a target node.

        Given a path from the current node to the target node, a list of node
        and target pairs is returned. This will contain either a single child
        node, next along the path, or all child nodes if the path is exhausted
        and the execute_on_children flag is set.

        Args:
            target: The path to the target from the current node.
            execute_on_children: If True, run on nodes beyond the target.
            ignore_exclusion: If True, traverse ALL nodes, including those
                marked as excluded (default: False).

        Returns:
            A list of (child, target) for each addressed child.
        """
        next_target_path = target.split("/")[1:]

        # Still more path to go, so find the next node along it.
        if next_target_path:
            targets = [
                (child, "/".join(next_target_path))
                for child in self.children_nodes
                if child.name == next_target_path[0]
                and (child.included or ignore_exclusion)
            ]
            if not targets:
                self.log.info(
                    f"'{next_target_path[0]}' is not a child of '{self.name}'"
                )
            if len(targets) > 1:
                self.log.warning(
                    f"Multiple children matched '{next_target_path[0]}' in '{self.name}'"
                )
            return targets

        # Handle execute_on_children only if the path is exhausted.
        if execute_on_children:
            return self.address_all(ignore_exclusion=ignore_exclusion)

        # Path is exhausted and we are NOT executing on children.
        return []

    def address_all(
        self,
        ignore_exclusion: bool = False,
    ) -> list[tuple[ChildNode, str]]:
        """Finds all child nodes.

        Returns a list of node and target pairs for each child node. The
        returned data is structured the same as that of address_target_path.

        Args:
            ignore_exclusion: If True, traverse ALL nodes, including those
                marked as excluded (default: False).

        Returns:
            A list of (child, target) for each addressed child.
        """
        return [
            (child, child.name)
            for child in self.children_nodes
            if child.included or ignore_exclusion
        ]

    @staticmethod
    def propagate_concurrently(
        child_command: Callable[[ChildNode, str], T],
        child_list: list[tuple[ChildNode, str]],
    ) -> list[T]:
        """Propagate commands concurrently to a list of children.

        Args:
            child_command: Callable to be executed for each child, with
                arguments (child, target).
            child_list: List of (node, target) for each addressed child.

        Returns:
            List of responses from each child.
        """
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(child_command, child_node, child_target)
                for child_node, child_target in child_list
            ]
            return [f.result() for f in as_completed(futures)]

    ########################################################
    ############# Status, description commands #############
    ########################################################

    @broadcasted
    @authentified_and_authorised(action=ActionType.READ, system=SystemType.CONTROLLER)
    @publish_command_time
    def status(
        self, request: AddressedCommand, context: ServicerContext
    ) -> StatusResponse:
        request.target = self.parse_target_string(request.target)
        response = StatusResponse(
            token=None,
            name=self.name,
        )

        # This node.
        if request.target == self.name or request.execute_along_path:
            status = get_status_message(self)
            response.status.CopyFrom(status)

        # Children nodes.
        def child_command(child: ChildNode, target: str) -> StatusResponse:
            return child.status(
                target,
                request.execute_along_path,
                request.execute_on_all_subsequent_children_in_path,
            )

        child_list = self.address_target_path(
            request.target,
            request.execute_on_all_subsequent_children_in_path,
        )
        child_responses = self.propagate_concurrently(child_command, child_list)
        response.children.extend(child_responses)

        response.flag = ResponseFlag.EXECUTED_SUCCESSFULLY

        return response

    @broadcasted
    @authentified_and_authorised(action=ActionType.READ, system=SystemType.CONTROLLER)
    @publish_command_time
    def describe(
        self, request: AddressedCommand, context: ServicerContext
    ) -> DescribeResponse:
        request.target = self.parse_target_string(request.target)
        response = DescribeResponse(
            token=None,
            name=self.name,
        )

        # This node.
        if request.target == self.name or request.execute_along_path:
            description = Description(
                type="controller",
                name=self.name,
                endpoint=self.uri if self.uri is not None else "unknown",
                info=get_detector_name(self.configuration),
                session=self.session,
                commands=None,
            )
            if broadcast_description := self.describe_broadcast():
                description.broadcast.Pack(broadcast_description)
            response.description.CopyFrom(description)

        # Children nodes.
        def child_command(child: ChildNode, target: str) -> DescribeResponse:
            return child.describe(
                target,
                request.execute_along_path,
                request.execute_on_all_subsequent_children_in_path,
            )

        child_list = self.address_target_path(
            request.target,
            request.execute_on_all_subsequent_children_in_path,
        )
        child_responses = self.propagate_concurrently(child_command, child_list)
        response.children.extend(child_responses)

        response.flag = ResponseFlag.EXECUTED_SUCCESSFULLY

        return response

    @broadcasted
    @authentified_and_authorised(action=ActionType.READ, system=SystemType.CONTROLLER)
    @publish_command_time
    def describe_fsm(
        self, request: AddressedCommand, context: ServicerContext
    ) -> DescribeFSMResponse:
        request.target = self.parse_target_string(request.target)
        response = DescribeFSMResponse(
            token=None,
            name=self.name,
        )

        # This node.
        if request.target == self.name or request.execute_along_path:
            payload = unpack_any(request.command_data, PlainText)
            if payload.text == "all-transitions":
                description = convert_fsm_transition(
                    self.stateful_node.get_all_fsm_transitions()
                )
            elif payload.text == "":
                description = convert_fsm_transition(
                    self.stateful_node.get_fsm_transitions()
                )
            else:
                all_transitions = self.stateful_node.get_all_fsm_transitions()
                interesting_transitions = []
                for transition in all_transitions:
                    if payload.text == transition.source:
                        interesting_transitions += [transition]
                    if payload.text == transition.name:
                        interesting_transitions += [transition]
                description = convert_fsm_transition(interesting_transitions)

            description.type = "controller"
            description.name = self.name
            description.session = self.session
            description.sequences.extend(self.stateful_node.get_fsm_sequences())
            response.description.CopyFrom(description)

        # Children nodes.
        def child_command(child: ChildNode, target: str) -> DescribeFSMResponse:
            return child.describe_fsm(
                target,
                request.execute_along_path,
                request.execute_on_all_subsequent_children_in_path,
            )

        child_list = self.address_target_path(
            request.target,
            request.execute_on_all_subsequent_children_in_path,
        )
        child_responses = self.propagate_concurrently(child_command, child_list)
        response.children.extend(child_responses)

        response.flag = ResponseFlag.EXECUTED_SUCCESSFULLY

        return response

    ########################################
    ############# FSM commands #############
    ########################################

    @broadcasted
    @authentified_and_authorised(action=ActionType.UPDATE, system=SystemType.CONTROLLER)
    @in_control
    @publish_command_time
    def execute_fsm_command(
        self,
        request: ExecuteFSMCommandRequest,
        context: ServicerContext,
    ) -> ExecuteFSMCommandResponse:
        command = request.command
        command_name = command.command_name
        self.log.debug(f"FSM command: {command}")

        transition = self.stateful_node.get_fsm_transition(command_name)
        self.log.debug(f"FSM transition: {transition}")

        response = ExecuteFSMCommandResponse(
            token=None,
            name=self.name,
            command_name=command_name,
        )

        # Check controller readiness.
        if not self.stateful_node.get_ready_state():
            self.log.error(
                f"Command '{command_name}' not executed: controller is not ready."
            )
            response.flag = ResponseFlag.NOT_EXECUTED_NOT_READY
            return response

        # Check if node is in error.
        if self.stateful_node.node_is_in_error():
            self.log.error(f"Command '{command_name}' not executed: node is in error.")
            response.fsm_flag = FSMResponseFlag.FSM_NOT_EXECUTED_IN_ERROR
            response.flag = ResponseFlag.EXECUTED_SUCCESSFULLY
            return response

        # Check if node is excluded.
        if not self.stateful_node.node_is_included():
            self.log.error(f"Command '{command_name}' not executed: node is excluded.")
            response.fsm_flag = FSMResponseFlag.FSM_NOT_EXECUTED_EXCLUDED
            response.flag = ResponseFlag.EXECUTED_SUCCESSFULLY
            return response

        # Check if transition is possible from current state.
        if not self.stateful_node.can_transition(transition):
            state = self.stateful_node.get_node_operational_state()
            self.log.error(
                f"Command '{command_name}' not executed: not possible from state '{state}'."
            )
            response.fsm_flag = FSMResponseFlag.FSM_INVALID_TRANSITION
            response.flag = ResponseFlag.EXECUTED_SUCCESSFULLY
            return response

        # Define what to do for child nodes.
        def child_command(child: ChildNode, target: str) -> ExecuteFSMCommandResponse:
            return child.execute_fsm_command(
                command,
                target,
                request.execute_along_path,
                request.execute_on_all_subsequent_children_in_path,
            )

        # This node.
        if request.target == self.name or request.execute_along_path:
            fsm_args = self.stateful_node.decode_fsm_arguments(command)
            fsm_data = self.stateful_node.prepare_transition(
                transition=transition,
                transition_args=fsm_args,
                transition_data=command.data,
                ctx=self,
            )

            # If the command publishes to ELisa Logbook, make sure that .dotdrunc.json
            # is present and well formatted
            using_elisa_logbook = "elisa-logbook" in self.fsm_config.get_actions()
            if using_elisa_logbook and command_name in ["start", "drain_dataflow"]:
                try:
                    get_dotdrunc_json()
                except (DotDruncJsonIncorrectFormat, DotDruncJsonNotFound) as e:
                    self.log.warning(f"ELisa Logbook entry will not be posted. {e}")

            if command_name == "start":
                self.controller_publisher(
                    message=RunInfo(
                        run_type=self.runinfo.get("production_vs_test", ""),
                        run_number=self.runinfo.get("run", 0),
                        disable_data_storage=self.runinfo.get(
                            "disable_data_storage", False
                        ),
                        trigger_rate=self.runinfo.get("trigger_rate", 0.0),
                        run_time_at_start=int(self.runinfo.get("run_time_at_start", 0)),
                        run_time_since_start=0,
                        run_config_file=self.configuration.oks_path,
                        run_config_name=self.configuration.oks_key.session,
                    ),
                    custom_origin=self.custom_origin,
                )

            # Begin propagating FSM transition to children.
            self.stateful_node.propagate_transition_mark(transition)

            child_list = self.address_target_path(
                request.target,
                request.execute_on_all_subsequent_children_in_path,
            )
            child_responses = self.propagate_concurrently(child_command, child_list)
            response.children.extend(child_responses)

            # Finish propagating FSM transition to children.
            self.stateful_node.finish_propagating_transition_mark(transition)

            # Start FSM transition on this node.
            self.stateful_node.start_transition_mark(transition)

            # Finish FSM transition on this node.
            self.stateful_node.terminate_transition_mark(transition)

            fsm_data = self.stateful_node.finalise_transition(
                transition=transition,
                transition_args=fsm_args,
                transition_data=fsm_data,
                ctx=self,
            )

            # Set FSM error flag based on child responses.
            response.fsm_flag = FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY
            for child_response in child_responses:
                if child_response.flag not in [
                    ResponseFlag.EXECUTED_SUCCESSFULLY,
                ] or child_response.fsm_flag not in [
                    FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY,
                    FSMResponseFlag.FSM_NOT_EXECUTED_EXCLUDED,
                ]:
                    response.fsm_flag = FSMResponseFlag.FSM_FAILED
                    self.stateful_node.to_error()
                    break

        # Children nodes.
        else:
            child_list = self.address_target_path(
                request.target,
                request.execute_on_all_subsequent_children_in_path,
            )
            child_responses = self.propagate_concurrently(child_command, child_list)
            response.children.extend(child_responses)

        response.flag = ResponseFlag.EXECUTED_SUCCESSFULLY

        return response

    @broadcasted
    @authentified_and_authorised(action=ActionType.UPDATE, system=SystemType.CONTROLLER)
    @in_control
    @publish_command_time
    def recompute_status(
        self, request: AddressedCommand, context: ServicerContext
    ) -> RecomputeStatusResponse:
        request.target = self.parse_target_string(request.target)
        response = RecomputeStatusResponse(
            token=None,
            name=self.name,
        )

        # This node.
        if request.target == self.name or request.execute_along_path:

            def child_command(child: ChildNode, target: str) -> StatusResponse:
                return child.recompute_status(
                    target,
                    request.execute_along_path,
                    request.execute_on_all_subsequent_children_in_path,
                )

            child_list = self.address_all()
            child_responses = self.propagate_concurrently(child_command, child_list)

            self_should_go_to_error = False
            children_states = set()
            children_sub_states = set()

            for s in child_responses:
                if s.flag != ResponseFlag.EXECUTED_SUCCESSFULLY:
                    self_should_go_to_error = True

                try:
                    child_status = s.status
                    children_states.add(child_status.state)
                    children_sub_states.add(child_status.sub_state)
                    if child_status.in_error:
                        self_should_go_to_error = True

                except UnpackingError as e:
                    self.log.error(
                        f"Failed to decode status for {s.name}: {e}, assuming it is excluded"
                    )
                    self_should_go_to_error = True
                    continue

            children_in_inconsistent_state = len(children_states) > 1
            children_in_inconsistent_sub_state = len(children_sub_states) > 1

            if (
                children_in_inconsistent_state
                or children_in_inconsistent_sub_state
                or self_should_go_to_error
            ) and not self.stateful_node.node_is_in_error():
                self.log.warning(
                    f"Children states: {children_states=}, {children_sub_states=}, the state is inconsistent or one node is in error, going to error"
                )
                self.stateful_node.to_error()

            if (
                not children_in_inconsistent_state
                and not children_in_inconsistent_sub_state
                and not self_should_go_to_error
            ):
                children_state = children_states.pop()
                children_sub_state = children_sub_states.pop()
                self.log.info(
                    f"Children state: {children_state}, children sub state: {children_sub_state}"
                )

                if children_sub_state == "idle":
                    children_sub_state = children_state

                self.stateful_node.resolve_error()
                self.stateful_node.force_set_node_operational_state(children_state)
                self.stateful_node.force_set_node_operational_sub_state(
                    children_sub_state
                )

            status = get_status_message(self)
            response.status.CopyFrom(status)

            def child_command(child: ChildNode, target: str) -> StatusResponse:
                return child.status(
                    target,
                    request.execute_along_path,
                    request.execute_on_all_subsequent_children_in_path,
                )

            child_list = self.address_all(ignore_exclusion=True)
            child_responses = self.propagate_concurrently(child_command, child_list)
            response.children.extend(child_responses)

        # Children nodes.
        else:

            def child_command(child: ChildNode, target: str) -> StatusResponse:
                return child.recompute_status(
                    target,
                    request.execute_along_path,
                    request.execute_on_all_subsequent_children_in_path,
                )

            child_list = self.address_target_path(
                request.target,
                request.execute_on_all_subsequent_children_in_path,
            )
            child_responses = self.propagate_concurrently(child_command, child_list)
            response.children.extend(child_responses)

        response.flag = ResponseFlag.EXECUTED_SUCCESSFULLY

        return response

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE, system=SystemType.CONTROLLER
    )  # 2nd step
    @in_control  # 3rd step
    @OLD_unpack_addressed_command_to()  # 4th step
    @publish_command_time
    def include(
        self,
        addressed_commands: dict[str, AddressedCommand],
        execute_on_self: bool,
        token: Token,
    ) -> PlainText:
        resp = None
        if execute_on_self:
            try:
                self.stateful_node.include_node()
            except CannotInclude:
                resp = PlainText(text=f"{self.name} is already included")
            else:
                resp = PlainText(text=f"{self.name} included")

        # Now we snoop into the addressed_commands and see if we can find a target that is a children, and include it
        for child_name, addressed_command in addressed_commands.items():
            for n in self.children_nodes:
                if n.name == addressed_command.target:
                    n.included = True

        response_children = self.OLD_propagate_to_children(
            "include",
            addressed_commands,
            token,
        )

        return Response(
            name=self.name,
            token=token,
            data=pack_to_any(resp) if resp else None,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            children=response_children,
        )

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE, system=SystemType.CONTROLLER
    )  # 2nd step
    @in_control
    @OLD_unpack_addressed_command_to()  # 3rd step
    @publish_command_time
    def exclude(
        self,
        addressed_commands: dict[str, AddressedCommand],
        execute_on_self: bool,
        token: Token,
    ) -> PlainText:
        resp = None
        if execute_on_self:
            try:
                self.stateful_node.exclude_node()
            except CannotExclude:
                resp = PlainText(text=f"{self.name} is already excluded")
            else:
                resp = PlainText(text=f"{self.name} excluded")

        # Now we snoop into the addressed_commands and see if we can find a target that is a children, and exclude it
        for child_name, addressed_command in addressed_commands.items():
            for n in self.children_nodes:
                if n.name == addressed_command.target:
                    n.included = False

        response_children = self.OLD_propagate_to_children(
            "exclude",
            addressed_commands,
            token,
        )

        return Response(
            name=self.name,
            token=token,
            data=pack_to_any(resp) if resp else None,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            children=response_children,
        )

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.EXPERT, system=SystemType.CONTROLLER
    )  # 2nd step
    @in_control
    @OLD_unpack_addressed_command_to(PlainText)  # 3rd step
    @publish_command_time
    def execute_expert_command(
        self,
        payload: PlainText,
        addressed_commands: dict[str, AddressedCommand],
        execute_on_self: bool,
        token: Token,
    ) -> Response:
        children_expert_command_response = self.OLD_propagate_to_children(
            "execute_expert_command",
            addressed_commands,
            token,
        )

        return Response(
            name=self.name,
            token=token,
            data=pack_to_any(PlainText(text=f"{self.name} propagated expert command")),
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            children=children_expert_command_response,
        )

    ##########################################
    ############# Actor commands #############
    ##########################################

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE, system=SystemType.CONTROLLER
    )  # 2nd step
    @OLD_unpack_addressed_command_to()  # 3rd step
    @publish_command_time
    def take_control(
        self,
        addressed_commands: dict[str, AddressedCommand],
        execute_on_self: bool,
        token: Token,
    ) -> Response:
        resp = ""
        if execute_on_self:
            if self.actor.take_control(token) != 0:
                resp += f"Could not take control on {self.name}"
            else:
                resp += f"{token.user_name} took control on {self.name}"

        response_children = self.OLD_propagate_to_children(
            "take_control",
            addressed_commands,
            token,
        )
        if any(
            cr.flag
            not in [
                ResponseFlag.EXECUTED_SUCCESSFULLY,
                ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
            ]
            for cr in response_children
        ):
            resp += ", could not take control for all children"

        return Response(
            name=self.name,
            token=token,
            data=pack_to_any(PlainText(text=resp)) if resp else None,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            children=response_children,
        )

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE, system=SystemType.CONTROLLER
    )  # 2nd step
    @in_control  # 3rd step
    @OLD_unpack_addressed_command_to()  # 4th step
    @publish_command_time
    def surrender_control(
        self,
        addressed_commands: dict[str, AddressedCommand],
        execute_on_self: bool,
        token: Token,
    ) -> Response:
        resp = ""
        if execute_on_self:
            user = self.actor.get_user_name()
            if self.actor.surrender_control(token) != 0:
                resp += f"Could not surrender control on {self.name}"
            else:
                resp += f"{user} surrendered control on {self.name}"

        response_children = self.OLD_propagate_to_children(
            "surrender_control",
            addressed_commands,
            token,
        )

        if any(
            cr.flag
            not in [
                ResponseFlag.EXECUTED_SUCCESSFULLY,
                ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
            ]
            for cr in response_children
        ):
            resp += ", could not surrender control for all children"

        return Response(
            name=self.name,
            token=token,
            data=pack_to_any(PlainText(text=resp)) if resp else None,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            children=response_children,
        )

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ, system=SystemType.CONTROLLER
    )  # 2nd step
    @OLD_unpack_addressed_command_to()  # 3rd step
    @publish_command_time
    def who_is_in_charge(
        self,
        addressed_commands: dict[str, AddressedCommand],
        execute_on_self: bool,
        token: Token,
    ) -> Response:
        if execute_on_self:
            user = pack_to_any(PlainText(text=self.actor.get_user_name()))
        else:
            user = None

        response_children = self.OLD_propagate_to_children(
            "who_is_in_charge",
            addressed_commands,
            token,
        )

        return Response(
            name=self.name,
            token=token,
            data=user,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            children=response_children,
        )

    ##########################################
    ####### Integration test commands ########
    ##########################################

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE, system=SystemType.CONTROLLER
    )  # 2nd step
    @in_control
    @OLD_unpack_addressed_command_to()  # 3rd step
    @publish_command_time
    def to_error(
        self,
        addressed_commands: dict[str, AddressedCommand],
        execute_on_self: bool,
        token: Token,
    ) -> PlainText:
        """
        Transitions the stateful node to an error state. Used for testing purposes.
        """
        try:
            if execute_on_self:
                self.stateful_node.to_error()

            response_children = self.OLD_propagate_to_children(
                "to_error",
                addressed_commands,
                token,
            )

            return Response(
                name=self.name,
                token=token,
                data=None,
                flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
                children=response_children,
            )
        except Exception as e:
            self.log.exception(e)
            return Response(
                name=self.name,
                token=token,
                data=None,
                flag=ResponseFlag.DRUNC_EXCEPTION_THROWN,
                children=None,
            )
