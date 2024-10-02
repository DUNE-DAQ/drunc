# Messaging format

We use [Google's Remote Procedural Call](https://grpc.io/) to handle network calls between the client and server. The format of the messages is protobuf.

## Command structure
Any command sent to a `drunc` server has the same basic structure, defined [here](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/request_response.proto):
```
message Request {
  Token token = 1;
  optional google.protobuf.Any data = 2;
}

message Response {
  string name = 1; // Name of the entity responding
  Token token = 2; // The token of the sender
  optional google.protobuf.Any data = 3; // Any arbitrary data returned by the command
  ResponseFlag flag = 4; // Whether the command was successfull ON SELF ONLY!
  repeated Response children = 5; // an array of the responses of the children, if there are children
}

enum ResponseFlag {
  EXECUTED_SUCCESSFULLY = 0;
  FAILED = 1;
  NOT_EXECUTED_NOT_IMPLEMENTED = 2;
  NOT_EXECUTED_NOT_IN_CONTROL = 3;
  NOT_EXECUTED_NOT_AUTHORISED = 4;
  DRUNC_EXCEPTION_THROWN = 5;
  UNHANDLED_EXCEPTION_THROWN = 6;
  NOT_EXECUTED_BAD_REQUEST_FORMAT = 7;
}

```

## `Token`s
Have the following form, defined [here](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/token.proto):
```
message Token {
  string token = 1;
  string user_name = 2;
}
```

Right now, this token can be anything, simply because the authorisation/authentication facilities are not integrated into `drunc`. In the future, this is likely going to be an SSO token from CERN or FNAL (or nothing, if you are not running at these places or do not want to have authorisation/authentication).

## Command data

The payload is a `google.protobuf.Any`, and it's not a required field. This does not mean that there is no structure for the payload, in fact, the payload is dictated by protobuf schemas too, but it should be packaged inside the data field of the `Request` to this format.

Similarly, the endpoint can answer (or not) with data, packaged inside an `Any` message.

# Command execution

Every time you send a command, `drunc` will check that you are allowed to execute the command, unpack the data to the appropriate format and try to execute it. If all these steps are successful you should get a response with the format above, data (optionally, if it makes sense to return any), the sender's token, the name of the process that executed it, a response flag and a vector of the response from the children.

You can send RPC from python, node, C++ [and many more](https://grpc.io/docs/).

## Exception handling

If any of the steps above fails, the RPC is interrupted, and a `Stacktrace` object is returned data is returned, you may otherwise get a `PlainText` message. The `Stacktrace` message has the [format](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/generic.proto):
```
message Stacktrace{
  repeated string text = 1;
}
```

and the `PlainText` is simply:
```
message PlainText {
  string text = 1;
}
```

# Self-description
All the drunc servers implement a `describe` RPC. They return a `Description` object of the format:

```
message Description {
  string type = 1;
  string name = 2;
  optional string session = 3;
  repeated CommandDescription commands = 4;
  optional google.protobuf.Any broadcast = 5;
}
```
 - `type` can be `process_manager` or `controller` right now it is a string.
 - `name` is the name of the server.
 - `session` is the (optional) session name.
 - `commands` is a vector of acceptable commands to send to the endpoint.
 - `broadcast` is a description of the broadcast service.

`CommandDescription` is used to describe all the commands, it has the following format:
```
message CommandDescription {
  string name = 1;
  repeated string data_type = 2;
  string help = 3;
  string return_type = 4;
}
```
 - `name` is the name of the RPC
 - `data_type` is the format of the `data` field that is expected inside the `Request`. This data is encoded in an `Any` format and decoded prior to command execution
 - `help` is a string providing help
 - `return_type` is the format of the `data` field that should be expected inside the `Response` if the command execution was successful.

For broadcasting, there is right now only one format that the `describe` command can fill, with a description of the Kafka service that is used to broadcast the log messages:
```
message KafkaBroadcastHandlerConfiguration{
  string kafka_address = 1;
  string topic = 2;
}
```
 - `kafka_address` is a bootstrap server
 - `topic` is the Kafka topic on which this service is logging.

More formats of broadcasting may be added in the future (potentially using [ERS's python binding](https://github.com/DUNE-DAQ/erskafka/tree/develop/python/erskafka)).

# Broadcasting
As mentioned earlier, the only working solution for broadcasting is Kafka. All messages feed to Kafka by drunc have the [form](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/broadcast.proto#L51):
```
message BroadcastMessage{
  Emitter emitter = 1;
  BroadcastType type = 2;
  google.protobuf.Any data = 3;
}
```
Where:
```
message Emitter {
  string process = 1;
  string session = 2;
}
```
and
```
enum BroadcastType {
  ACK                             = 0;
  RECEIVER_REMOVED                = 1;
  RECEIVER_ADDED                  = 2;
  SERVER_READY                    = 3;
  SERVER_SHUTDOWN                 = 4;
  TEXT_MESSAGE                    = 15;
  COMMAND_EXECUTION_START         = 5;
  COMMAND_RECEIVED                = 16;
  COMMAND_EXECUTION_SUCCESS       = 6;
  EXCEPTION_RAISED                = 7;
  UNHANDLED_EXCEPTION_RAISED      = 8;
  STATUS_UPDATE                   = 9;
  SUBPROCESS_STATUS_UPDATE        = 10;
  DEBUG                           = 11;
  CHILD_COMMAND_EXECUTION_START   = 12;
  CHILD_COMMAND_EXECUTION_SUCCESS = 13;
  CHILD_COMMAND_EXECUTION_FAILED  = 14;
  FSM_STATUS_UPDATE               = 17;
}
```

Note that for now, the `data` field is only filled with [PlainText](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/generic.proto#L7C1-L9C2) messages.

In the future, this may change.
