# Interfacing the Process Manager

The Process Manager uses gRPC to talk between client and server.

## Request/response interactions
The Process Manager consumes [`druncschema.Request`](https://github.com/DUNE-DAQ/druncschema/blob/develop/proto/request_response.proto#L6) that holds:
 - token,
 - any data payload.

The token has a format description in [`druncschema.Token`](https://github.com/DUNE-DAQ/druncschema/blob/develop/proto/token.proto#L3):
 - The token is any random sequence of characters for now, this will be changed in future version of the run control with something meaningful.
 - The other entry is the username.

For the payload, one can wrap any protobuf schema in it with the following snippet (python):
```python
from google.protobuf.any_pb2 import Any
data = Any()
data.Pack(payload)
```
if the `payload` is itself a protobuf objects.

Once a request is consumed, the Process Manager returns a [`druncschema.Response`](https://github.com/DUNE-DAQ/druncschema/blob/develop/proto/request_response.proto#L11). This object has:
- token,
- any data payload.

The token has the same format as above, it is a copy of the one from the request.

The data payload is again a protobuf object. It can be decoded with the following snippet (python):
```python
if not data.Is(format.DESCRIPTOR):
    raise Exception(f'Cannot unpack {data} into {format}')
req = format()
data.Unpack(req)
```
where `format` is the name of the class that it should be decoded to.

## Request/stream interactions
The Process Manager can also stream data out. In this case it consumes a `druncschema.Request` and returns a [`druncschema.Stream`](https://github.com/DUNE-DAQ/druncschema/blob/develop/proto/request_response.proto#L16). This object is very similar to the `druncschema.Response`.


## Interactions
In this section we go over all the interactions implemented and describe them.

### Boot
Request/response type.

The request's payload should be of the form

  rpc restart      (Request) returns (Response) {}
  rpc is_alive     (Request) returns (Response) {}
  rpc kill         (Request) returns (Response) {}
  rpc killall      (Request) returns (Response) {}
  rpc flush        (Request) returns (Response) {}
  rpc list_process (Request) returns (Response) {}
  rpc logs         (Request) returns (stream LogLine) {}