
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

        log.debug(f'Sending: {command} to the controller, with {request}')

        response = cmd(request)

    except grpc.RpcError as e:
        log.error(f'Error sending command {command} to controller: {e.code().name}')
        log.error(e)
        try:
            log.error(e.details())
        except:
            pass

        if rethrow:
            raise e
        return None

    return response



def get_status_message(stateful):
    from druncschema.controller_pb2 import Status
    return Status(
        name = stateful.name,
        uri = stateful.uri,
        bool   ping,
        string state,
        in_error,
        included
    )

