
def unpack_any(data, format):
    if not data.Is(format.DESCRIPTOR):
        print(f'Cannot unpack {data} into {format}')
        from drunc.communication import controller_pb2 as ctler_excpt
        raise ctler_excpt.MalformedMessage()
    req = format()
    data.Unpack(req)
    return req
