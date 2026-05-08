import multiprocessing
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, TypeVar

from daqpytools.logging import LogHandlerConf, setup_daq_ers_logger
from druncschema.authoriser_pb2 import ActionType, SystemType
from druncschema.broadcast_pb2 import BroadcastType
from druncschema.controller_pb2 import (
    DescribeFSMRequest,
    DescribeFSMResponse,
    DescribeRequest,
    DescribeResponse,
    ExcludeRequest,
    ExcludeResponse,
    ExecuteExpertCommandRequest,
    ExecuteExpertCommandResponse,
    ExecuteFSMCommandRequest,
    ExecuteFSMCommandResponse,
    FSMCommand,
    FSMResponseFlag,
    IncludeRequest,
    IncludeResponse,
    RecomputeStatusRequest,
    RecomputeStatusResponse,
    StatusRequest,
    StatusResponse,
    SurrenderControlRequest,
    SurrenderControlResponse,
    TakeControlRequest,
    TakeControlResponse,
    ToErrorRequest,
    ToErrorResponse,
    WhoIsInChargeRequest,
    WhoIsInChargeResponse,
)
from druncschema.controller_pb2_grpc import ControllerServicer
from druncschema.description_pb2 import Description
from druncschema.opmon.FSM_pb2 import FSMStatus
from druncschema.opmon.generic_pb2 import RunInfo
from druncschema.request_response_pb2 import ResponseFlag
from druncschema.token_pb2 import Token
from grpc import ServicerContext

from drunc.authoriser.configuration import DummyAuthoriserConfHandler
from drunc.authoriser.decorators import authentified_and_authorised
from drunc.authoriser.dummy_authoriser import DummyAuthoriser
from drunc.broadcast.server.broadcast_sender import BroadcastSender
from drunc.broadcast.server.configuration import BroadcastSenderConfHandler
from drunc.broadcast.server.decorators import broadcasted
from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.connectivity_service.exceptions import ApplicationLookupUnsuccessful
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
from drunc.fsm.actions.utils import get_dotdrunc_json
from drunc.fsm.configuration import FSMConfHandler
from drunc.fsm.exceptions import (
    DotDruncJsonIncorrectFormat,
    DotDruncJsonNotFound,
)
from drunc.fsm.utils import convert_fsm_transition
from drunc.utils.utils import get_logger

T = TypeVar("T")


class Controller(ControllerServicer):
    children_nodes: List[ChildNode] = []

    def __init__(self, configuration, name: str, session: str, token: Token):
        """C'tor. Note that controllers require the ERS variables defined
        in OKS to exist as env variables!"""
        super().__init__()

        self._previous_error_state = False
        self.name = name
        self.session = session
        self.broadcast_service = None
        self.monitoring_metrics = ControllerMonitoringMetrics()
        self.handlerconf = LogHandlerConf(init_ers=True)
        self.log = get_logger(f"controller.core.{name}_ctrl")
        setup_daq_ers_logger(self.log, session, f"drunc.{name}_ctrl")
        log_init = get_logger("controller.core.__init__")
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
        self.stop_event: threading.Event | None = None
        self.thread: threading.Thread | None = None
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

    def init_controller(self) -> None:
        log_init_controller = get_logger("controller.core.init_controller")
        log_init_controller.info("Finishing initialisation of controller")

        try:
            log_init_controller.info("Initializing the controller children")
            self.children_nodes = self.configuration.init_children(
                session_name=self.session,
                init_token=self.actor.get_token(),
                connectivity_service=self.connectivity_service,
            )
        except ApplicationLookupUnsuccessful:
            log_init_controller.error(
                "Failed to find all child applications on the connectivity service. Check that all children are up and registered to the connectivity service."
            )
            self.stateful_node.to_error()
            return

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
            child_list = self.address_all()
            child_responses = self.propagate_concurrently(
                lambda child, target: child.status(target), child_list
            )

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
            child.take_control(execute_on_all_subsequent_children_in_path=True)

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
        if isinstance(message, FSMStatus) and message.in_error:
            if message.in_error and not self._previous_error_state:
                self.log.error(
                    f"{self.name} is now in an error state", extra=self.handlerconf.ERS
                )
            elif not message.in_error and self._previous_error_state:
                self.log.info(
                    f"{self.name} is now in a good state", extra=self.handlerconf.ERS
                )
            self._previous_error_state = message.in_error
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

            if self.stop_event.wait(timeout=interval_s):
                break

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
        self.log.info(f"Terminating controller {self.name}")
        self.running = False

        if hasattr(self, "connectivity_service") and self.connectivity_service:
            if self.connectivity_service_thread:
                self.connectivity_service_thread.join()
            self.log.info("Unregistering from the connectivity service")
            self.connectivity_service.retract(self.name + "_control", fail_quickly=True)

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

        if self.opmon_publisher and self.stop_event:
            self.log.debug("Stopping opmon publisher")
            try:
                self.stop_event.set()
                if self.thread:
                    self.thread.join(timeout=1.0)
                    if self.thread.is_alive():
                        self.log.debug(
                            "OpMon publisher thread did not stop within timeout, continuing shutdown"
                        )
                    else:
                        self.log.debug("opmon publisher stopped")
            except Exception as e:
                self.log.warning(f"Error stopping opmon publisher: {e}")

        self.log.debug("Threading threads")
        for t in threading.enumerate():
            self.log.debug(f"{t.name} TID: {t.native_id} is_alive: {t.is_alive}")

        with multiprocessing.Manager() as manager:
            self.log.debug("Multiprocess threads")
            self.log.debug(manager.list())

    def __del__(self):
        self.terminate()

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
        include_excluded_nodes: bool = False,
    ) -> list[tuple[ChildNode, str]]:
        """Finds the next node(s) along a given path to a target node.

        Given a path from the current node to the target node, a list of node
        and target pairs is returned. This will contain either a single child
        node, next along the path, or all child nodes if the path is exhausted
        and the execute_on_children flag is set.

        Args:
            target: The path to the target from the current node.
            execute_on_children: If True, run on nodes beyond the target.
            include_excluded_nodes: If True, traverse ALL nodes, including
                those marked as excluded (default: False).

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
                and (child.included or include_excluded_nodes)
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
            return self.address_all(include_excluded_nodes=include_excluded_nodes)

        # Path is exhausted and we are NOT executing on children.
        return []

    def address_all(
        self,
        include_excluded_nodes: bool = False,
    ) -> list[tuple[ChildNode, str]]:
        """Finds all child nodes.

        Returns a list of node and target pairs for each child node. The
        returned data is structured the same as that of address_target_path.

        Args:
            include_excluded_nodes: If True, traverse ALL nodes, including
                those marked as excluded (default: False).

        Returns:
            A list of (child, target) for each addressed child.
        """
        return [
            (child, child.name)
            for child in self.children_nodes
            if child.included or include_excluded_nodes
        ]

    @staticmethod
    def propagate_concurrently(
        child_callable: Callable[[ChildNode, str], T],
        child_list: list[tuple[ChildNode, str]],
    ) -> list[T]:
        """Propagate commands concurrently to a list of children.

        Args:
            child_callable: Callable to be executed for each child, with
                arguments (child, target).
            child_list: List of (node, target) for each addressed child.

        Returns:
            List of responses from each child.
        """
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(child_callable, child_node, child_target)
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
        self, request: StatusRequest, context: ServicerContext
    ) -> StatusResponse:
        response = StatusResponse(
            token=None,
            name=self.name,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        try:
            # Parse and validate target.
            request.target = self.parse_target_string(request.target)
        except ValueError:
            response.flag = ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT
            return response

        # This node.
        if request.target == self.name or request.execute_along_path:
            status = get_status_message(self)
            response.status.CopyFrom(status)

        # Children nodes (ignore exclusion).
        child_list = self.address_target_path(
            request.target,
            request.execute_on_all_subsequent_children_in_path,
            include_excluded_nodes=True,
        )
        child_responses = self.propagate_concurrently(
            lambda child, target: child.status(
                target,
                request.execute_along_path,
                request.execute_on_all_subsequent_children_in_path,
            ),
            child_list,
        )
        response.children.extend(child_responses)

        return response

    @broadcasted
    @authentified_and_authorised(action=ActionType.READ, system=SystemType.CONTROLLER)
    @publish_command_time
    def describe(
        self, request: DescribeRequest, context: ServicerContext
    ) -> DescribeResponse:
        response = DescribeResponse(
            token=None,
            name=self.name,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        try:
            # Parse and validate target.
            request.target = self.parse_target_string(request.target)
        except ValueError:
            response.flag = ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT
            return response

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

        # Children nodes (ignore exclusion).
        child_list = self.address_target_path(
            request.target,
            request.execute_on_all_subsequent_children_in_path,
            include_excluded_nodes=True,
        )
        child_responses = self.propagate_concurrently(
            lambda child, target: child.describe(
                target,
                request.execute_along_path,
                request.execute_on_all_subsequent_children_in_path,
            ),
            child_list,
        )
        response.children.extend(child_responses)

        return response

    @broadcasted
    @authentified_and_authorised(action=ActionType.READ, system=SystemType.CONTROLLER)
    @publish_command_time
    def describe_fsm(
        self, request: DescribeFSMRequest, context: ServicerContext
    ) -> DescribeFSMResponse:
        response = DescribeFSMResponse(
            token=None,
            name=self.name,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        try:
            # Parse and validate target.
            request.target = self.parse_target_string(request.target)
        except ValueError:
            response.flag = ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT
            return response

        # This node.
        if request.target == self.name or request.execute_along_path:
            if request.key == "all-transitions":
                description = convert_fsm_transition(
                    self.stateful_node.get_all_fsm_transitions()
                )
            elif request.key == "":
                description = convert_fsm_transition(
                    self.stateful_node.get_fsm_transitions()
                )
            else:
                all_transitions = self.stateful_node.get_all_fsm_transitions()
                interesting_transitions = []
                for transition in all_transitions:
                    if request.key == transition.source:
                        interesting_transitions += [transition]
                    if request.key == transition.name:
                        interesting_transitions += [transition]
                description = convert_fsm_transition(interesting_transitions)

            description.type = "controller"
            description.name = self.name
            description.session = self.session
            description.sequences.extend(self.stateful_node.get_fsm_sequences())
            response.description.CopyFrom(description)

        # Children nodes (ignore exclusion).
        child_list = self.address_target_path(
            request.target,
            request.execute_on_all_subsequent_children_in_path,
            include_excluded_nodes=True,
        )
        child_responses = self.propagate_concurrently(
            lambda child, target: child.describe_fsm(
                target,
                request.execute_along_path,
                request.execute_on_all_subsequent_children_in_path,
                request.key,
            ),
            child_list,
        )
        response.children.extend(child_responses)

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
        response = ExecuteFSMCommandResponse(
            token=None,
            name=self.name,
            command_name=request.command.command_name,
            fsm_flag=FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        try:
            # Parse and validate target.
            request.target = self.parse_target_string(request.target)
        except ValueError:
            response.flag = ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT
            return response

        command = request.command
        command_name = command.command_name

        fsm_cmd_log = f"FSM command run: {command_name} for target {request.target} "
        if command_name == "start" and (
            cmd := self.stateful_node.decode_fsm_arguments(command)
        ):
            fsm_cmd_log += f"with arguments {cmd}"
        elif command_name == "stop":
            fsm_cmd_log += f"for run number {self.runinfo.get('run', 'unknown')}"
        self.log.info(fsm_cmd_log)
        transition = self.stateful_node.get_fsm_transition(command_name)
        self.log.debug(f"FSM transition: {transition}")

        # Check controller readiness.
        if not self.stateful_node.get_ready_state():
            self.log.error(
                f"Command '{command_name}' not executed: controller is not ready."
            )
            response.fsm_flag = FSMResponseFlag.FSM_FAILED
            response.flag = ResponseFlag.NOT_EXECUTED_NOT_READY
            return response

        # Check if node is in error.
        if self.stateful_node.node_is_in_error():
            self.log.error(
                f"Command '{command_name}' not executed: node is in error.",
                extra=self.handlerconf.ERS,
            )
            response.fsm_flag = FSMResponseFlag.FSM_NOT_EXECUTED_IN_ERROR
            return response

        # Check if node is excluded.
        if not self.stateful_node.node_is_included():
            self.log.error(f"Command '{command_name}' not executed: node is excluded.")
            response.fsm_flag = FSMResponseFlag.FSM_NOT_EXECUTED_EXCLUDED
            return response

        # Check if transition is possible from current state.
        if not self.stateful_node.can_transition(transition):
            state = self.stateful_node.get_node_operational_state()
            self.log.error(
                f"Command '{command_name}' not executed: not possible from state '{state}'."
            )
            response.fsm_flag = FSMResponseFlag.FSM_INVALID_TRANSITION
            return response

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

            # Child FSMCommands are *NOT THE SAME* as the parent one!
            # TODO: this is quite misleading and error prone. Needs looking into.
            child_command = FSMCommand()
            child_command.CopyFrom(command)
            child_command.data = fsm_data

            child_list = self.address_target_path(
                request.target,
                request.execute_on_all_subsequent_children_in_path,
            )
            child_responses = self.propagate_concurrently(
                lambda child, target: child.execute_fsm_command(
                    child_command,
                    target,
                    request.execute_along_path,
                    request.execute_on_all_subsequent_children_in_path,
                ),
                child_list,
            )
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
            for child_response in child_responses:
                if child_response.flag not in [
                    ResponseFlag.EXECUTED_SUCCESSFULLY,
                    ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
                ] or child_response.fsm_flag not in [
                    FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY,
                    FSMResponseFlag.FSM_NOT_EXECUTED_EXCLUDED,
                    FSMResponseFlag.FSM_INVALID_TRANSITION,
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
            child_responses = self.propagate_concurrently(
                lambda child, target: child.execute_fsm_command(
                    command,
                    target,
                    request.execute_along_path,
                    request.execute_on_all_subsequent_children_in_path,
                ),
                child_list,
            )
            response.children.extend(child_responses)

        return response

    @broadcasted
    @authentified_and_authorised(action=ActionType.EXPERT, system=SystemType.CONTROLLER)
    @in_control
    @publish_command_time
    def execute_expert_command(
        self,
        request: ExecuteExpertCommandRequest,
        context: ServicerContext,
    ) -> ExecuteExpertCommandResponse:
        response = ExecuteExpertCommandResponse(
            token=None,
            name=self.name,
            fsm_flag=FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        try:
            # Parse and validate target.
            request.target = self.parse_target_string(request.target)
        except ValueError:
            response.flag = ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT
            return response

        # This node.
        response.data = f"'{self.name}' propagated expert command"

        # Children nodes.
        child_list = self.address_target_path(
            request.target,
            request.execute_on_all_subsequent_children_in_path,
        )
        child_responses = self.propagate_concurrently(
            lambda child, target: child.execute_expert_command(
                request.json_string,
                target,
                request.execute_along_path,
                request.execute_on_all_subsequent_children_in_path,
            ),
            child_list,
        )
        response.children.extend(child_responses)

        return response

    @broadcasted
    @authentified_and_authorised(action=ActionType.UPDATE, system=SystemType.CONTROLLER)
    @in_control
    @publish_command_time
    def include(
        self,
        request: IncludeRequest,
        context: ServicerContext,
    ) -> IncludeResponse:
        response = IncludeResponse(
            token=None,
            name=self.name,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        try:
            # Parse and validate target.
            request.target = self.parse_target_string(request.target)
        except ValueError:
            response.flag = ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT
            return response

        # This node.
        if request.target == self.name or request.execute_along_path:
            try:
                self.stateful_node.include_node()
            except CannotInclude:
                response.text = f"'{self.name}' is already included"
            else:
                response.text = f"'{self.name}' included"

        # Children nodes (ignore exclusion).
        child_list = self.address_target_path(
            request.target,
            request.execute_on_all_subsequent_children_in_path,
            include_excluded_nodes=True,
        )
        child_responses = self.propagate_concurrently(
            lambda child, target: child.include(
                target,
                request.execute_along_path,
                request.execute_on_all_subsequent_children_in_path,
            ),
            child_list,
        )
        response.children.extend(child_responses)

        return response

    @broadcasted
    @authentified_and_authorised(action=ActionType.UPDATE, system=SystemType.CONTROLLER)
    @in_control
    @publish_command_time
    def exclude(
        self,
        request: ExcludeRequest,
        context: ServicerContext,
    ) -> ExcludeResponse:
        response = ExcludeResponse(
            token=None,
            name=self.name,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        try:
            # Parse and validate target.
            request.target = self.parse_target_string(request.target)
        except ValueError:
            response.flag = ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT
            return response

        # This node.
        if request.target == self.name or request.execute_along_path:
            try:
                self.stateful_node.exclude_node()
            except CannotExclude:
                response.text = f"'{self.name}' is already excluded"
            else:
                response.text = f"'{self.name}' excluded"

        # Children nodes (ignore exclusion).
        child_list = self.address_target_path(
            request.target,
            request.execute_on_all_subsequent_children_in_path,
            include_excluded_nodes=True,
        )
        child_responses = self.propagate_concurrently(
            lambda child, target: child.exclude(
                target,
                request.execute_along_path,
                request.execute_on_all_subsequent_children_in_path,
            ),
            child_list,
        )
        response.children.extend(child_responses)

        return response

    @broadcasted
    @authentified_and_authorised(action=ActionType.UPDATE, system=SystemType.CONTROLLER)
    @in_control
    @publish_command_time
    def recompute_status(
        self, request: RecomputeStatusRequest, context: ServicerContext
    ) -> RecomputeStatusResponse:
        response = RecomputeStatusResponse(
            token=None,
            name=self.name,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        try:
            # Parse and validate target.
            request.target = self.parse_target_string(request.target)
        except ValueError:
            response.flag = ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT
            return response

        # Children nodes.
        child_list = self.address_target_path(
            request.target,
            request.execute_on_all_subsequent_children_in_path,
        )
        child_responses = self.propagate_concurrently(
            lambda child, target: child.recompute_status(
                target,
                request.execute_along_path,
                request.execute_on_all_subsequent_children_in_path,
            ),
            child_list,
        )
        response.children.extend(child_responses)

        # This node.
        if request.target == self.name or request.execute_along_path:
            # Query status of immediate children only (except excluded).
            child_status_list = self.address_all()
            child_status_responses = self.propagate_concurrently(
                lambda child, target: child.status(target, False, False),
                child_status_list,
            )

            children_states = set()
            children_sub_states = set()
            children_in_error = False

            for s in child_status_responses:
                if s.flag != ResponseFlag.EXECUTED_SUCCESSFULLY:
                    children_in_error = True
                child_status = s.status
                children_states.add(child_status.state)
                children_sub_states.add(child_status.sub_state)
                if child_status.in_error:
                    children_in_error = True

            children_in_bad_state = (
                children_in_error
                or len(children_states) > 1
                or len(children_sub_states) > 1
            )

            if children_in_bad_state:
                self.log.debug(f"{children_states=}, {children_sub_states=}")
                self.log.warning(
                    "Child nodes are in error or inconsistent state. Going to error."
                )

                # Children are in bad state, so set our state to error.
                self.stateful_node.to_error()

            else:
                state = children_states.pop()
                sub_state = children_sub_states.pop()
                self.log.debug(f"{state=}, {sub_state=}")

                if sub_state == "idle":
                    sub_state = state

                # All is well, so fix our state.
                self.stateful_node.resolve_error()
                self.stateful_node.force_set_node_operational_state(state)
                self.stateful_node.force_set_node_operational_sub_state(sub_state)

        return response

    ##########################################
    ############# Actor commands #############
    ##########################################

    @broadcasted
    @authentified_and_authorised(action=ActionType.UPDATE, system=SystemType.CONTROLLER)
    @publish_command_time
    def take_control(
        self,
        request: TakeControlRequest,
        context: ServicerContext,
    ) -> TakeControlResponse:
        response = TakeControlResponse(
            token=None,
            name=self.name,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        try:
            # Parse and validate target.
            request.target = self.parse_target_string(request.target)
        except ValueError:
            response.flag = ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT
            return response

        text = ""

        # Children nodes (ignore exclusion).
        child_list = self.address_target_path(
            request.target,
            request.execute_on_all_subsequent_children_in_path,
            include_excluded_nodes=True,
        )
        child_responses = self.propagate_concurrently(
            lambda child, target: child.take_control(
                target,
                request.execute_along_path,
                request.execute_on_all_subsequent_children_in_path,
            ),
            child_list,
        )
        response.children.extend(child_responses)

        # This node.
        if request.target == self.name or request.execute_along_path:
            if self.actor.take_control(request.token) == 0:
                text += f"took control on {self.name}"
            else:
                text += f"Could not take control on {self.name}"

        if any(
            cr.flag
            not in [
                ResponseFlag.EXECUTED_SUCCESSFULLY,
                ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
            ]
            for cr in child_responses
        ):
            text += ", could not take control of all children"

        if text:
            response.text = text

        return response

    @broadcasted
    @authentified_and_authorised(action=ActionType.UPDATE, system=SystemType.CONTROLLER)
    @in_control
    @publish_command_time
    def surrender_control(
        self,
        request: SurrenderControlRequest,
        context: ServicerContext,
    ) -> SurrenderControlResponse:
        response = SurrenderControlResponse(
            token=None,
            name=self.name,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        try:
            # Parse and validate target.
            request.target = self.parse_target_string(request.target)
        except ValueError:
            response.flag = ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT
            return response

        text = ""

        # Children nodes (ignore exclusion).
        child_list = self.address_target_path(
            request.target,
            request.execute_on_all_subsequent_children_in_path,
            include_excluded_nodes=True,
        )
        child_responses = self.propagate_concurrently(
            lambda child, target: child.surrender_control(
                target,
                request.execute_along_path,
                request.execute_on_all_subsequent_children_in_path,
            ),
            child_list,
        )
        response.children.extend(child_responses)

        # This node.
        if request.target == self.name or request.execute_along_path:
            if self.actor.surrender_control(request.token) == 0:
                text += f"surrendered control on {self.name}"
            else:
                text += f"Could not surrender control on {self.name}"

        if any(
            cr.flag
            not in [
                ResponseFlag.EXECUTED_SUCCESSFULLY,
                ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
            ]
            for cr in child_responses
        ):
            text += ", could not surrender control of all children"

        if text:
            response.text = text

        return response

    @broadcasted
    @authentified_and_authorised(action=ActionType.READ, system=SystemType.CONTROLLER)
    @publish_command_time
    def who_is_in_charge(
        self,
        request: WhoIsInChargeRequest,
        context: ServicerContext,
    ) -> WhoIsInChargeResponse:
        response = WhoIsInChargeResponse(
            token=None,
            name=self.name,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        try:
            # Parse and validate target.
            request.target = self.parse_target_string(request.target)
        except ValueError:
            response.flag = ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT
            return response

        # Children nodes (ignore exclusion).
        child_list = self.address_target_path(
            request.target,
            request.execute_on_all_subsequent_children_in_path,
            include_excluded_nodes=True,
        )
        child_responses = self.propagate_concurrently(
            lambda child, target: child.who_is_in_charge(
                target,
                request.execute_along_path,
                request.execute_on_all_subsequent_children_in_path,
            ),
            child_list,
        )
        response.children.extend(child_responses)

        # This node.
        if request.target == self.name or request.execute_along_path:
            response.text = self.actor.get_user_name()

        return response

    ##########################################
    ####### Integration test commands ########
    ##########################################

    @broadcasted
    @authentified_and_authorised(action=ActionType.UPDATE, system=SystemType.CONTROLLER)
    @in_control
    @publish_command_time
    def to_error(
        self,
        request: ToErrorRequest,
        context: ServicerContext,
    ) -> ToErrorResponse:
        """
        Transitions the stateful node to an error state. Used for testing purposes.
        """
        response = ToErrorResponse(
            token=None,
            name=self.name,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        try:
            # Parse and validate target.
            request.target = self.parse_target_string(request.target)
        except ValueError:
            response.flag = ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT
            return response

        # Children nodes (ignore exclusion).
        child_list = self.address_target_path(
            request.target,
            request.execute_on_all_subsequent_children_in_path,
            include_excluded_nodes=True,
        )
        child_responses = self.propagate_concurrently(
            lambda child, target: child.to_error(
                target,
                request.execute_along_path,
                request.execute_on_all_subsequent_children_in_path,
            ),
            child_list,
        )
        response.children.extend(child_responses)

        # This node.
        if request.target == self.name or request.execute_along_path:
            self.stateful_node.to_error()

        return response
