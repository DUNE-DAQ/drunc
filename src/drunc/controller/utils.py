import grpc
from druncschema.controller_pb2 import Status
from druncschema.generic_pb2 import PlainText, Stacktrace
from druncschema.request_response_pb2 import Request
from google.protobuf import any_pb2
from grpc_status import rpc_status

from drunc.controller.stateful_node import StatefulNode
from drunc.utils.grpc_utils import rethrow_if_unreachable_server, unpack_any
from drunc.utils.utils import get_logger


def get_status_message(stateful: StatefulNode):
    state_string = stateful.get_node_operational_state()
    if state_string != stateful.get_node_operational_sub_state():
        state_string += f" ({stateful.get_node_operational_sub_state()})"

    return Status(
        state=state_string,
        sub_state=stateful.get_node_operational_sub_state(),
        in_error=stateful.node_is_in_error(),
        included=stateful.node_is_included(),
    )


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
