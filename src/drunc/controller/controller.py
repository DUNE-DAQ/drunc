import multiprocessing
import threading
import time
import traceback
from typing import List, Optional

from druncschema.authoriser_pb2 import ActionType, SystemType
from druncschema.broadcast_pb2 import BroadcastType
from druncschema.controller_pb2 import (
    AddressedCommand,
    FSMCommand,
    FSMCommandResponse,
    FSMResponseFlag,
    Status,
)
from druncschema.controller_pb2_grpc import ControllerServicer
from druncschema.generic_pb2 import PlainText, Stacktrace
from druncschema.opmon.generic_pb2 import RunInfo
from druncschema.request_response_pb2 import Description, Response, ResponseFlag
from druncschema.token_pb2 import Token
from google.protobuf.any_pb2 import Any

from drunc.authoriser.configuration import DummyAuthoriserConfHandler
from drunc.authoriser.decorators import authentified_and_authorised
from drunc.authoriser.dummy_authoriser import DummyAuthoriser
from drunc.broadcast.server.broadcast_sender import BroadcastSender
from drunc.broadcast.server.configuration import BroadcastSenderConfHandler
from drunc.broadcast.server.decorators import broadcasted
from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.controller.children_interface.child_node import ChildNode
from drunc.controller.children_interface.rest_api_child import ResponseListener
from drunc.controller.decorators import (
    in_control,
    publish_command_time,
    unpack_addressed_command_to,
)
from drunc.controller.exceptions import CannotSurrenderControl
from drunc.controller.stateful_node import CannotExclude, CannotInclude, StatefulNode
from drunc.controller.utils import (
    ControllerMonitoringMetrics,
    get_detector_name,
    get_status_message,
)
from drunc.exceptions import DruncException
from drunc.fsm.configuration import FSMConfHandler
from drunc.fsm.utils import convert_fsm_transition
from drunc.utils.grpc_utils import UnpackingError, pack_to_any, unpack_any
from drunc.utils.utils import get_logger


class ControllerActor:
    def __init__(self, token: Optional[Token] = None):
        self.log = get_logger("controller.actor")
        self._token = Token(token="", user_name="")
        if token is not None:
            self._token.CopyFrom(token)
        self._lock = threading.Lock()

    def get_token(self) -> Token:
        return self._token

    def get_user_name(self) -> str:
        return self._token.user_name

    def _update_actor(self, token: Optional[Token] = Token()) -> None:
        self._lock.acquire()
        self._token.CopyFrom(token)
        self._lock.release()

    def compare_token(self, token1, token2):
        self._lock.acquire()
        result = (
            token1.user_name == token2.user_name and token1.token == token2.token
        )  #!! come on protobuf, you can compare messages
        self._lock.release()
        return result

    def token_is_current_actor(self, token):
        return self.compare_token(token, self._token)

    def surrender_control(self, token) -> None:
        if self.compare_token(self._token, token):
            self._update_actor(Token())
            return
        raise CannotSurrenderControl(
            f"Token {token} cannot release control of {self._token}"
        )

    def take_control(self, token) -> None:
        # if not self.compare_token(self._token, token):
        #     raise OtherUserAlreadyInControl(f'Actor {self._token.user_name} is already in control')
        self._update_actor(token)
        return 0


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

        fsmch = FSMConfHandler(
            data=self.configuration.data.controller.fsm,
        )

        self.stateful_node = StatefulNode(
            fsm_configuration=fsmch,
            publisher=self.controller_publisher,
            init_state="initialising",
            name=name,
            session=session,
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

    def init_controller(self):
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
            children_statuses = self.propagate_to_all_children(
                command_name="status",
                token=self.actor.get_token(),
            )
            children_states = {}
            for response in children_statuses:
                in_error = False
                try:
                    status = unpack_any(response.data, Status)
                    in_error = status.in_error
                    children_states[response.name] = status.state
                except UnpackingError:
                    log_init_controller.error(
                        f"Failed to unpack status from {response.name}:"
                    )
                    if response.data.Is(Stacktrace.DESCRIPTOR):
                        stack = unpack_any(response.data, Stacktrace)
                        for line in stack.text:
                            log_init_controller.error(f"{response.name}: {line}")
                    elif response.data.Is(PlainText.DESCRIPTOR):
                        log_init_controller.error(
                            f"{response.name}: {unpack_any(response.data, PlainText).text}"
                        )
                    else:
                        log_init_controller.error(
                            f"{response.name}: Unknown data type: {type(response.data)}"
                        )

                if in_error:
                    self.stateful_node.to_error()
                    break

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
            child.propagate_command("take_control", None, self.actor.get_token())

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

    def async_interrupt_with_exception(self, *args, **kwargs):
        return self.broadcast_service._async_interrupt_with_exception(*args, **kwargs)

    def controller_publisher(self, message, custom_origin: dict | None = None):
        if self.opmon_publisher is not None:
            try:
                if custom_origin is None:
                    custom_origin = {}

                self.opmon_publisher.publish(
                    message=message,
                    custom_origin=custom_origin,
                )
                self.log.debug(f"Published {type(message)} to OpMon")
            except Exception as e:
                self.log.error(f"Failed to publish to OpMon: {e}")

    def threading_publish_state(self, interval_s: float = 10.0):
        run_time_at_start = 0
        while not self.stop_event.is_set():
            try:
                self.log.debug(f"Publishing periodic FSM status every {interval_s}s")
                self.stateful_node.publish_state()
                current_state = self.stateful_node.get_node_operational_state()

                if current_state in ("none", "initial", "configured"):
                    self.monitoring_metrics = ControllerMonitoringMetrics()

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

                if run_time_at_start:
                    self.monitoring_metrics.run_time_since_start = int(
                        time.time() - run_time_at_start
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
                    )
                )
            except Exception as e:
                self.log.exception(f"Error while publishing periodic status: {e}")
            time.sleep(interval_s)

    def construct_error_node_response(
        self, command_name: str, token: Token, cause: FSMResponseFlag
    ) -> Response:
        fsm_result = FSMCommandResponse(
            flag=cause,
            command_name=command_name,
        )

        return Response(
            name=self.name,
            token=token,
            data=pack_to_any(fsm_result),
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            children=[],
        )

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
            self.log.debug(f"{t.getName()} TID: {t.native_id} is_alive: {t.is_alive}")

        with multiprocessing.Manager() as manager:
            self.log.debug("Multiprocess threads")
            self.log.debug(manager.list())

    def __del__(self):
        self.terminate()

    def propagate_to_all_children(
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

        return self.propagate_addressed_command(
            command_name=command_name,
            addressed_commands=addressed_commands,
            token=token,
            override_payload=command_data,
        )

    def propagate_addressed_command(
        self,
        command_name: str,
        addressed_commands: dict[str, AddressedCommand],
        token: Token,
        override_payload: Any = None,
    ):
        self.log.debug(f"Propagating {command_name} to children")
        response_children = []
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

            try:
                command_data_str = str(command_data).replace("\n", " ")
                self.log.debug(
                    f"Propagating {command_name} to child {child.name}, command data: {command_data_str}, token: {token}"
                )
                response = child.propagate_command(
                    command=command_name,
                    data=command_data,
                    token=token,
                )
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
                    "command_data": data if not override_payload else override_payload,
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

    ########################################################
    ############# Status, description commands #############
    ########################################################

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ, system=SystemType.CONTROLLER
    )  # 2nd step
    @unpack_addressed_command_to()  # 3rd step
    @publish_command_time
    def status(
        self,
        addressed_commands: dict[str, AddressedCommand],
        execute_on_self: bool,
        token: Token,
    ) -> Response:
        status = None
        if execute_on_self:
            status = pack_to_any(get_status_message(self))

        children_statuses = self.propagate_addressed_command(
            "status",
            addressed_commands=addressed_commands,
            token=token,
        )

        return Response(
            name=self.name,
            token=token,
            data=status,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            children=children_statuses,
        )

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ, system=SystemType.CONTROLLER
    )  # 2nd step
    @unpack_addressed_command_to()  # 3rd step
    @publish_command_time
    def describe(
        self,
        addressed_commands: dict[str, AddressedCommand],
        execute_on_self: bool,
        token: Token,
    ) -> Response:
        d = None

        if execute_on_self:
            bd = self.describe_broadcast()
            d = Description(
                type="controller",
                name=self.name,
                endpoint=self.uri if self.uri is not None else "unknown",
                info=get_detector_name(self.configuration),
                session=self.session,
                # commands=self.commands,
            )
            if bd:
                d.broadcast.CopyFrom(pack_to_any(bd))

        children_description = self.propagate_addressed_command(
            "describe",
            addressed_commands=addressed_commands,
            token=token,
        )

        return Response(
            name=self.name,
            token=token,
            data=pack_to_any(d) if d else None,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            children=children_description,
        )

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.READ, system=SystemType.CONTROLLER
    )  # 2nd step
    @unpack_addressed_command_to(PlainText)  # 4th step
    @publish_command_time
    def describe_fsm(
        self,
        payload: PlainText,
        addressed_commands: dict[str, AddressedCommand],
        execute_on_self: bool,
        token: Token,
    ) -> Response:
        desc = None
        if execute_on_self:
            if payload.text == "all-transitions":
                desc = convert_fsm_transition(
                    self.stateful_node.get_all_fsm_transitions()
                )
            elif payload.text == "":
                desc = convert_fsm_transition(self.stateful_node.get_fsm_transitions())
            else:
                all_transitions = self.stateful_node.get_all_fsm_transitions()
                interesting_transitions = []
                for transition in all_transitions:
                    if input.text == transition.source:
                        interesting_transitions += [transition]
                    if input.text == transition.name:
                        interesting_transitions += [transition]
                desc = convert_fsm_transition(interesting_transitions)
            desc.type = "controller"
            desc.name = self.name
            desc.session = self.session

        children_description = self.propagate_addressed_command(
            "describe_fsm",
            addressed_commands=addressed_commands,
            token=token,
        )

        return Response(
            name=self.name,
            token=token,
            data=pack_to_any(desc) if desc else None,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            children=children_description,
        )

    ########################################
    ############# FSM commands #############
    ########################################
    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE, system=SystemType.CONTROLLER
    )  # 2nd step
    @in_control  # 3rd step
    @unpack_addressed_command_to(FSMCommand)  # 4th step
    @publish_command_time
    def execute_fsm_command(
        self,
        payload: FSMCommand,
        addressed_commands: dict[str, AddressedCommand],
        execute_on_self: bool,
        token: Token,
    ) -> Response:
        if not self.stateful_node.get_ready_state():
            self.log.warning("Controller is not ready, not executing command")
            return Response(
                name=self.name,
                token=token,
                data=None,
                flag=ResponseFlag.NOT_EXECUTED_NODE_IN_ERROR,  ### TODO: Add a ResponseFlag.NOT_EXECUTED_NOT_READY
                children=[],
            )

        if execute_on_self:
            if self.stateful_node.node_is_in_error():
                return self.construct_error_node_response(
                    payload.command_name,
                    token,
                    cause=FSMResponseFlag.FSM_NOT_EXECUTED_IN_ERROR,
                )

            if not self.stateful_node.node_is_included():
                self.log.error(
                    f"Node is not included, not executing command {payload.command_name}."
                )
                fsm_result = FSMCommandResponse(
                    flag=FSMResponseFlag.FSM_NOT_EXECUTED_EXCLUDED,
                    command_name=payload.command_name,
                )

                return Response(
                    name=self.name,
                    token=token,
                    data=pack_to_any(fsm_result),
                    flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
                    children=[],
                )

            transition = self.stateful_node.get_fsm_transition(payload.command_name)

            self.log.debug(f'The transition requested is "{str(transition)}"')

            if not self.stateful_node.can_transition(transition):
                self.log.error(
                    f'Cannot "{transition.name}" as this is an invalid command in state "{self.stateful_node.get_node_operational_state()}"'
                )

                fsm_result = FSMCommandResponse(
                    flag=FSMResponseFlag.FSM_INVALID_TRANSITION,
                    command_name=payload.command_name,
                )

                return Response(
                    name=self.name,
                    token=token,
                    data=pack_to_any(fsm_result),
                    flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
                    children=[],
                )

            self.log.debug(f"FSM command data: {payload}")

            fsm_args = self.stateful_node.decode_fsm_arguments(payload)

            fsm_data = self.stateful_node.prepare_transition(
                transition=transition,
                transition_args=fsm_args,
                transition_data=payload.data,
                ctx=self,
            )
            if payload.command_name == "start":
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
                    )
                )

            self.stateful_node.propagate_transition_mark(transition)

            children_fsm_commands = {}
            for target, command in addressed_commands.items():
                child = next((c for c in self.children_nodes if c.name == target), None)
                if child is None:
                    self.log.error(f"Child {target} not found")
                    continue
                if not child.included:
                    self.log.info(
                        f"Child {target} is not included, not executing command {payload.command_name}."
                    )
                    continue

                child_fsm_command = FSMCommand()
                child_fsm_command.CopyFrom(unpack_any(command.command_data, FSMCommand))
                child_fsm_command.data = fsm_data

                children_fsm_commands[target] = AddressedCommand(
                    command_name=command.command_name,
                    command_data=pack_to_any(child_fsm_command),
                    target=target,
                    execute_along_path=command.execute_along_path,
                    execute_on_all_subsequent_children_in_path=command.execute_on_all_subsequent_children_in_path,
                )

            response_children = self.propagate_addressed_command(
                "execute_fsm_command",
                addressed_commands=children_fsm_commands,
                token=token,
            )

            child_worst_response_flag = ResponseFlag.EXECUTED_SUCCESSFULLY
            child_worst_fsm_flag = FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY

            for response_child in response_children:
                if response_child.flag != ResponseFlag.EXECUTED_SUCCESSFULLY:
                    child_worst_response_flag = response_child.flag
                    continue

                fsm_response = unpack_any(response_child.data, FSMCommandResponse)

                if fsm_response.flag not in [
                    FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY,
                    FSMResponseFlag.FSM_NOT_EXECUTED_EXCLUDED,
                ]:
                    child_worst_fsm_flag = fsm_response.flag

            self.stateful_node.finish_propagating_transition_mark(transition)

            self.stateful_node.start_transition_mark(transition)

            self.stateful_node.terminate_transition_mark(transition)

            fsm_data = self.stateful_node.finalise_transition(
                transition=transition,
                transition_args=fsm_args,
                transition_data=fsm_data,
                ctx=self,
            )
            if (
                child_worst_response_flag != ResponseFlag.EXECUTED_SUCCESSFULLY
                or child_worst_fsm_flag != FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY
            ):
                self.stateful_node.to_error()

            self_response_fsm_flag = child_worst_fsm_flag
            if child_worst_response_flag != ResponseFlag.EXECUTED_SUCCESSFULLY:
                self_response_fsm_flag = (
                    FSMResponseFlag.FSM_FAILED
                )  ## TODO: Add a FSMResponseFlag.FSM_COMMAND_ON_CHILD_FAILED

            fsm_result = FSMCommandResponse(
                flag=self_response_fsm_flag,
                command_name=payload.command_name,
            )

            return Response(
                name=self.name,
                token=token,
                data=pack_to_any(fsm_result),
                flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
                children=response_children,
            )
        else:
            return Response(
                name=self.name,
                token=token,
                data=None,
                flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
                children=self.propagate_addressed_command(
                    "execute_fsm_command",
                    addressed_commands=addressed_commands,
                    token=token,
                ),
            )

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE, system=SystemType.CONTROLLER
    )  # 2nd step
    @in_control
    @unpack_addressed_command_to()  # 3rd step
    @publish_command_time
    def recompute_status(
        self,
        addressed_commands: dict[str, AddressedCommand],
        execute_on_self: bool,
        token: Token,
    ) -> Response:
        if execute_on_self:
            statuses = self.propagate_to_all_children(
                "recompute_status",
                command_data=None,
                token=token,
                only_included=True,
            )

            self_should_go_to_error = False
            children_states = set()
            children_sub_states = set()

            for s in statuses:
                if s.flag != ResponseFlag.EXECUTED_SUCCESSFULLY:
                    self_should_go_to_error = True

                try:
                    status = unpack_any(s.data, Status)
                    children_states.add(status.state)
                    children_sub_states.add(status.sub_state)
                    if status.in_error:
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

            status = get_status_message(self.stateful_node)

            post_statuses = self.propagate_to_all_children(
                "status",
                command_data=None,
                token=token,
                only_included=False,
            )
            return Response(
                name=self.name,
                token=token,
                data=pack_to_any(status),
                flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
                children=post_statuses,
            )

        else:
            return Response(
                name=self.name,
                token=token,
                data=None,
                flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
                children=self.propagate_addressed_command(
                    "recompute_status",
                    addressed_commands=addressed_commands,
                    token=token,
                ),
            )

    # ORDER MATTERS!
    @broadcasted  # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.UPDATE, system=SystemType.CONTROLLER
    )  # 2nd step
    @in_control  # 3rd step
    @unpack_addressed_command_to()  # 4th step
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

        response_children = self.propagate_addressed_command(
            "include",
            addressed_commands=addressed_commands,
            token=token,
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
    @unpack_addressed_command_to()  # 3rd step
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

        response_children = self.propagate_addressed_command(
            "exclude",
            addressed_commands=addressed_commands,
            token=token,
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
    @unpack_addressed_command_to(PlainText)  # 3rd step
    @publish_command_time
    def execute_expert_command(
        self,
        payload: PlainText,
        addressed_commands: dict[str, AddressedCommand],
        execute_on_self: bool,
        token: Token,
    ) -> Response:
        children_expert_command_response = self.propagate_addressed_command(
            "execute_expert_command",
            addressed_commands=addressed_commands,
            token=token,
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
    @unpack_addressed_command_to()  # 3rd step
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

        response_children = self.propagate_addressed_command(
            "take_control",
            addressed_commands=addressed_commands,
            token=token,
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
    @unpack_addressed_command_to()  # 4th step
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

        response_children = self.propagate_addressed_command(
            "surrender_control",
            addressed_commands=addressed_commands,
            token=token,
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
    @unpack_addressed_command_to()  # 3rd step
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

        response_children = self.propagate_addressed_command(
            "who_is_in_charge",
            addressed_commands=addressed_commands,
            token=token,
        )

        return Response(
            name=self.name,
            token=token,
            data=user,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            children=response_children,
        )
