import time
from dataclasses import dataclass
from typing import cast

import grpc
from grpc_status import rpc_status

from drunc.utils.grpc_utils import rethrow_if_unreachable_server, unpack_any
from drunc.utils.utils import get_logger

from druncschema.controller_pb2 import Status, RunInfo  # isort: skip
from druncschema.generic_pb2 import PlainText, Stacktrace  # isort: skip
from druncschema.request_response_pb2 import Request  # isort: skip


def get_status_message(controller):
    stateful = controller.stateful_node
    state_string = stateful.get_node_operational_state()
    # if state_string != stateful.get_node_operational_sub_state():
    #     state_string += f" ({stateful.get_node_operational_sub_state()})"

    msg = Status(
        state=state_string,
        sub_state=stateful.get_node_operational_sub_state(),
        in_error=stateful.node_is_in_error(),
        included=stateful.node_is_included(),
    )

    if state_string in ("initial", "configured"):
        return msg

    if controller.runinfo and controller.runinfo.get("run", None) is not None:
        run_time_since_start = 0
        run_time_at_start = controller.runinfo.get("run_time_at_start", 0)
        if run_time_at_start:
            run_time_since_start = int(time.time() - run_time_at_start)

        msg.run_info.CopyFrom(
            RunInfo(
                run_type=controller.runinfo.get("production_vs_test", ""),
                trigger_rate=controller.runinfo.get("trigger_rate", 0.0),
                run_number=controller.runinfo["run"],
                disable_data_storage=controller.runinfo.get(
                    "disable_data_storage", False
                ),
                run_time_at_start=int(controller.runinfo.get("run_time_at_start", 0)),
                run_time_since_start=run_time_since_start,
                run_config_file=controller.configuration.oks_path,
                run_config_name=controller.configuration.oks_key.session,
            )
        )

    return msg


def get_detector_name(configuration) -> str:
    detector_name = None
    log = get_logger("controller.get_detector_name")
    if hasattr(configuration.data, "contains") and len(configuration.data.contains) > 0:
        if len(configuration.data.contains) > 0:
            log.debug(
                f"Application {configuration.data.id} has multiple contains, using the first one"
            )
        detector_name = (
            configuration.data.contains[0].id.replace("-", "_").replace("_", " ")
        )
    else:
        log.debug(
            f'Application {configuration.data.id} has no "contains" relation, hence no detector'
        )
    return detector_name


def handle_controller_grpc_error(error: grpc.RpcError) -> None:
    """Handle gRPC errors from sending commands to the controller.

    Args:
        error: The gRPC error to handle.
    """
    rethrow_if_unreachable_server(error)

    # RpcError is also a subclass of Call, and can be used in from_call.
    # The type stubs in types-grpcio do not reflect this, so we must cast.
    # See https://github.com/grpc/grpc/issues/10885.
    status = rpc_status.from_call(cast(grpc.Call, error))

    log = get_logger("controller.handle_controller_grpc_error")
    log.error("Error sending command to controller")

    if hasattr(status, "message"):
        log.error(status.message)

    if hasattr(status, "details"):
        for detail in status.details:
            if detail.Is(Stacktrace.DESCRIPTOR):
                text = "Stacktrace on remote server!\n"
                stack = unpack_any(detail, Stacktrace)
                for l in stack.text:
                    text += l + "\n"
                log.error(text)
            elif detail.Is(PlainText.DESCRIPTOR):
                text = unpack_any(detail, PlainText)
                log.error(text)

    raise error


def send_command(controller, token, command: str, data=None, rethrow=False):
    log = get_logger("controller.send_command")

    # Grab the command from the controller stub in the context
    # Add the token to the data (which can be of any protobuf type)
    # Send the command to the controller

    if not controller:
        raise RuntimeError("No controller initialised")

    cmd = getattr(controller, command)  # this throws if the command doesn't exist

    request = Request(token=token)
    if data is not None:
        request.data.Pack(data)

    log.debug(f"Sending: {command} to the controller, with {request=}")

    try:
        response = cmd(request)
    except grpc.RpcError as e:
        handle_controller_grpc_error(e)

    return response


def get_segment_lookup_timeout(segment_conf, base_timeout=60):
    def recurse_segment(segment, recursion_count: int = 1) -> int:
        if segment.segments == []:
            return recursion_count

        max_recursion = 0
        for child_segment in segment.segments:
            child_recursion_count = recurse_segment(child_segment, recursion_count + 1)
            if child_recursion_count > max_recursion:
                max_recursion = child_recursion_count
        return max_recursion

    recursion_count = recurse_segment(segment_conf, 1)
    return base_timeout * recursion_count


@dataclass
class ControllerMonitoringMetrics:
    """Store the metrics that the OpMon Controller publishes"""

    run_type: str = ""
    trigger_rate: float = 0.0
    run_number: int = 0
    disable_data_storage: bool = False
    run_time_at_start: int = 0
    run_time_since_start: int = 0
