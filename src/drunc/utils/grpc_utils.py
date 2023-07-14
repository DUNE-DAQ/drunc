

class UnpackingError(Exception):
    def __init__(self, data, format):
        self.data = data
        self.format = format
        super().__init__(f'Cannot unpack {data} to {format.DESCRIPTOR}')


def unpack_any(data, format):
    if not data.Is(format.DESCRIPTOR):
        raise UnpackingError(data, format)
    req = format()
    data.Unpack(req)
    return req


def send_command(controller, token, command:str, data=None, paths=[], recursive=False, rethrow=False):
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
        request = Request(
            token = token
        )

        if data:
            data_detail = any_pb2.Any()
            data_detail.Pack(data)
            request.data.CopyFrom(data_detail)

        log.debug(f'Sending: {command} with {request}')

        response = cmd(request)

    except grpc.RpcError as e:
        log.error(f'Error sending command {command}: {e.code().name}')
        log.error(e)
        try:
            log.error(e.details())
        except:
            pass

        if rethrow:
            raise e
        return None

    return response
