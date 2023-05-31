class MalformedMessage(Exception):
    pass

def unpack_any(data, format):
    if not data.Is(format.DESCRIPTOR):
        print(f'Cannot unpack {data} into {format}')
        from druncschema import controller_pb2 as ctler_excpt
        raise MalformedMessage
    req = format()
    data.Unpack(req)
    return req
