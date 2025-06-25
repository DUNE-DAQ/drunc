import re
import time
from dataclasses import dataclass

import grpc
from google.protobuf import any_pb2
from grpc_status import rpc_status

from drunc.controller.exceptions import DruncCommandException
from drunc.utils.grpc_utils import rethrow_if_unreachable_server, unpack_any
from drunc.utils.utils import get_logger

from druncschema.controller_pb2 import Status, RunInfo  # isort: skip
from druncschema.generic_pb2 import PlainText, Stacktrace  # isort: skip
from druncschema.request_response_pb2 import Request  # isort: skip
from druncschema.controller_pb2 import AddressedCommand  # isort: skip


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


def send_command(controller, token, command: str, data=None, rethrow=False):
    log = get_logger("controller.send_command")

    # Grab the command from the controller stub in the context
    # Add the token to the data (which can be of any protobuf type)
    # Send the command to the controller

    if not controller:
        raise RuntimeError("No controller initialised")

    cmd = getattr(controller, command)  # this throws if the command doesn't exist

    request = Request(token=token)

    try:
        if data:
            data_detail = any_pb2.Any()
            data_detail.Pack(data)
            request.data.CopyFrom(data_detail)
        log.debug(f"Sending: {command} to the controller, with {request=}")
        response = cmd(request)

    except grpc.RpcError as e:
        rethrow_if_unreachable_server(e)
        status = rpc_status.from_call(e)
        log.error(f'Error sending command "{command}" to controller')

        if hasattr(status, "message"):
            log.error(status.message)

        if hasattr(status, "details"):
            for detail in status.details:
                if detail.Is(Stacktrace.DESCRIPTOR):
                    # text = '[bold red]Stacktrace on remote server![/bold red]\n' # Temporary - bold red doesn't work
                    text = "Stacktrace on remote server!\n"
                    stack = unpack_any(detail, Stacktrace)
                    for l in stack.text:
                        text += l + "\n"
                elif detail.Is(PlainText.DESCRIPTOR):
                    txt = unpack_any(detail, PlainText)
                log.error(txt)

        if rethrow:
            raise e
        return None

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


# #/root-controller/df-controller execute_along_path=False

# {
#     "df-01": {
#         AddressedCommand(
#             target="/df-controller/df-01",
#             execute_along_path=True,
#             execute_on_all_subsequent_children_in_path=True,
#         )
#     }
# }


def address_command(
    obj,
    command_name,
    command_data,
    target,
    execute_along_path,
    execute_on_all_subsequent_children_in_path,
):
    log = get_logger("controller.address_command")

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


@dataclass
class ControllerMonitoringMetrics:
    """Store the metrics that the OpMon Controller publishes"""

    run_type: str = ""
    trigger_rate: float = 0.0
    run_number: int = 0
    disable_data_storage: bool = False
    run_time_at_start: int = 0
    run_time_since_start: int = 0
