

class UnpackingError(Exception):
    def __init__(self, data, format):
        self.data = data
        self.format = format
        super().__init__(f'Cannot unpack {data} to {format.DESCRIPTOR}')

def pack_to_any(data):
    from google.protobuf import any_pb2
    any = any_pb2.Any()
    any.Pack(data)
    return any

def unpack_any(data, format):
    if not data.Is(format.DESCRIPTOR):
        raise UnpackingError(data, format)
    req = format()
    data.Unpack(req)
    return req
