
def send_command(controller, token, command:str, data=None, paths=[], recursive=False, rethrow=False):
    from druncschema.controller_pb2 import ControllerRequest, Location
    from druncschema.request_response_pb2 import Request
    from druncschema.token_pb2 import Token
    import grpc
    from google.protobuf import any_pb2

    from drunc.utils.utils import get_logger

    log = get_logger("send_command")
    # Grab the command from the controller stub in the context
    # Add the token to the data (which can be of any protobuf type)
    # Send the command to the controller

    if not controller:
        raise RuntimeError('No controller initialised')

    cmd = getattr(controller, command) # this throws if the command doesn't exist

    token = Token()
    token.CopyFrom(token) # some protobuf magic

    try:
        locs = [
            Location(
                path = path,
                recursive = recursive
            ) for path in paths
        ]

        creq = ControllerRequest(
            location = locs
        )

        if data:
            data_detail = any_pb2.Any()
            data_detail.Pack(data)
            creq.data.CopyFrom(data_detail)


        request = Request(
            token = token,
        )
        creq_any = any_pb2.Any()
        creq_any.Pack(creq)
        request.data.CopyFrom(creq_any)

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
