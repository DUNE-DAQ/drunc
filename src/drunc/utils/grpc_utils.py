from drunc.communication.controller_pb2 import Request, Response, Token
import grpc
from google.protobuf import any_pb2



def unpack_any(data, format):
    if not data.Is(format.DESCRIPTOR):
        print(f'Cannot unpack {data} into {format}')
        from drunc.communication import controller_pb2 as ctler_excpt
        raise ctler_excpt.MalformedMessage()
    req = format()
    data.Unpack(req)
    return req




def send_command(controller, token:Token, command:str, data, rethrow=False) -> Response:
    from drunc.utils.utils import setup_fancy_logging

    log = setup_fancy_logging("SendCommand")
    # Grab the command from the controller stub in the context
    # Add the token to the data (which can be of any protobuf type)
    # Send the command to the controller

    if not controller:
        raise RuntimeError('No controller initialised')

    cmd = getattr(controller, command) # this throws if the command doesn't exist

    token = Token()
    token.CopyFrom(token) # some protobuf magic

    try:
        request = Request(token = token)
        if data:
            data_detail = any_pb2.Any()
            data_detail.Pack(data)
            request.data.CopyFrom(data_detail)

        log.debug(f'Sending: {command} to the controller, with {request}')

        response = cmd(request)

    except grpc.RpcError as e:
        log.error(f'Error sending command {command} to controller: {e.code().name}')
        log.error(e.details())
        if rethrow:
            raise e
        return None

    return response
