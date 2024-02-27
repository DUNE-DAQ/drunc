from drunc.controller.stateful_node import StatefulNode

def get_status_message(stateful:StatefulNode):
    from druncschema.controller_pb2 import Status
    state_string = stateful.get_node_operational_state()
    if state_string != stateful.get_node_operational_sub_state():
        state_string += f' ({stateful.get_node_operational_sub_state()})'

    return Status(
        name = stateful.name,
        state = state_string,
        sub_state = stateful.get_node_operational_sub_state(),
        in_error = stateful.node_is_in_error(),
        included = stateful.node_is_included(),
    )

def send_command(controller, token, command:str, data=None, rethrow=False):
    from druncschema.request_response_pb2 import Request
    import grpc
    from google.protobuf import any_pb2

    import logging
    log = logging.getLogger("send_command")

    # Grab the command from the controller stub in the context
    # Add the token to the data (which can be of any protobuf type)
    # Send the command to the controller

    if not controller:
        raise RuntimeError('No controller initialised')

    cmd = getattr(controller, command) # this throws if the command doesn't exist

    request = Request(
        token = token,
    )

    try:
        if data:
            data_detail = any_pb2.Any()
            data_detail.Pack(data)
            request.data.CopyFrom(data_detail)

        log.debug(f'Sending: {command} to the controller, with {request=}')

        response = cmd(request)
    except grpc.RpcError as e:
        from drunc.utils.grpc_utils import rethrow_if_unreachable_server
        rethrow_if_unreachable_server(e)

        from grpc_status import rpc_status
        status = rpc_status.from_call(e)

        log.error(f'Error sending command "{command}" to controller')

        from druncschema.generic_pb2 import Stacktrace, PlainText
        from drunc.utils.grpc_utils import unpack_any

        if hasattr(status, 'message'):
            log.error(status.message)

        if hasattr(status, 'details'):
            for detail in status.details:
                if detail.Is(Stacktrace.DESCRIPTOR):
                    text = 'Stacktrace [bold red]on remote server![/]\n'
                    stack = unpack_any(detail, Stacktrace)
                    for l in stack.text:
                        text += l+"\n"
                    log.error(text, extra={"markup": True})
                elif detail.Is(PlainText.DESCRIPTOR):
                    txt = unpack_any(detail, PlainText)
                    log.error(txt)

        if rethrow:
            raise e
        return None

    return response