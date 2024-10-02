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
The Process Manager can also stream data out. In this case it consumes a `druncschema.Request` and returns a `druncschema.Response` multiple times.

## Interactions
In this section we go over all the interactions implemented and describe them.


### boot
Request/response type.

The request's payload should be of the form [`druncschema.BootRequest`](https://github.com/DUNE-DAQ/druncschema/blob/develop/proto/process_manager.proto#L76).
This `BootRequest` has 2 entries:
- process_description (ProcessDescription)
- process_restriction (ProcessRestriction)

The process description is a `druncschema.ProcessDescription` object, that contains:
- a string to string map of the `env`,
- an `executable_and_arguments` vector (themselves in an protobuf object),
- process metadata.

`druncschema.ProcessMetadata` objects hold the following information:
- the name of the user who started the process: `user`,
- the name of the session which the process belongs to: `session`,
- a friendly name for the process (that the user provided): `name`,
- the `uuid` which is a unique identifier for any process.

`druncschema.ProcessRestriction` hold two entries:
- `allowed_hosts`, a vector of allowed hostnames,
- `allowed_host_types`, a vector of allowed host types, which for now isn't used.

The response from a boot RPC is of the form `druncschema.Response` with the payload being `druncschema.ProcessUUID`. This uuid is used to uniquely identify a process, right now, it is a very simple object holding a list of random characters.

The RPC will raise an error if the process manager couldn't boot the application.


### restart
Request/response type.

The request's payload is a [`druncschema.ProcessQuery`](https://github.com/DUNE-DAQ/druncschema/blob/develop/proto/process_manager.proto#L42).
It is used to query the processes in the process manager.

A query takes the following arguments:
- `uuids`
- `user`
- `session`
- `names`

`uuids` is a vector of unique identifiers for the process as described earlier, `user` is the user who started the process, `session` is the which the process belongs to, `names` is the friendly name in the metadata, note that names can have some python regex in them, so for example you can access all the processes by setting `names = [.*]`.

Restart will check that the query you have formulated corresponds to one an only one process, and restart the process.


### ps
Request/response type.

This request payload again uses a `druncschema.ProcessQuery` described earlier. This time however the query can correspond to many processes.
The answer's payload is of the form `druncschema.ProcessInstanceList`. This is a list of `druncschema.ProcessInstance`. Each of them hold the following information:
- `process_description` described earlier,
- `process_restriction` described earlier too,
- `status_code` which is an enum (running=0, dead=1),
- `return_code` the code with which the process exited, optionally,
- `uuid` the process unique identifier.


### kill
Request/response type.

This request payload again uses a `druncschema.ProcessQuery` described earlier. The query can match more than one process.
The answer's payload is of the form `druncschema.ProcessInstance` (described earlier), which is returned after the process is killed.


### flush
Request/response type.

This request payload again uses a `druncschema.ProcessQuery` described earlier. The query can match more than one process.
The answer's payload is of the form `druncschema.ProcessInstanceList` (described earlier), which is returned after the dead processes are flushed from process manager's memory. This command is useful to remove the old dead process that still appear in the "list_process" command.


### logs
Request/stream type.

This request payload again uses a `druncschema.LogRequest`, it contains:
- `query` a process query, which must match one and only one process
- `how_far`, an int32, which governs how far back we want the logs.

The answer's payload is of the form `druncschema.LogLine` (just a string containing a line), that is streamed for all the lines of the log.
