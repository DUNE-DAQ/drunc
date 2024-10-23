# `drunc-process-manager` interface

[This page](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Messaging-format) provides an entry point to the way the drunc processes should be interfaced.

This document refers to the Process manager in particular. Its proto file can be found [here](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/process_manager.proto)

## Messages
Remember that all the messages sent to and received from the process manager follow the `drunc` schema described [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Interfacing-with-drunc#command-structure). All the messages described here will end up in some form or another in the `data` fields of the `Request` and `Response` after being `Any`fied.

Note that the `ProcessManager` does not have children, so the `children` field will always be an empty array in the `Response`.

### `ProcessRestriction`
This message represents a set of hosts one which a process is allowed to run. This has not been particularly well thought off, but the assumption here is that there will either be a list of acceptable hosts or a list of groups of hosts on which the process can be run.

```
message ProcessRestriction {
  repeated string allowed_hosts = 1;
  repeated string allowed_host_types = 2;
}
```

### `LogRequest`
This message is used to request logs from processes. It uses a `query` ([ProcessQuery]((https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processuuid)) to figure out which process to get the logs from. The `how_far` integer is the number of lines the user wants back.

```
message LogRequest {
  ProcessQuery query = 1;
  int32 how_far = 2;
}
```

### `LogLine`
This is a line from the logs. `uuid` is the process unique identifier ([ProcessUUID](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processuuid)), and `line` is the text of the log.

```
message LogLine {
  ProcessUUID uuid = 1;
  string line = 2;
}
```

### `ProcessUUID`
This is a process unique identifier, encoded in a string.

```
message ProcessUUID {
  string uuid = 1;
}
```

### `ProcessMetadata`
This message encodes all the metadata used for a process. In theory, this is not strictly needed by the process to be executed, but I realise there is a `user` field here, which does not fall in that category... Oh well.
 - `uuid` is the process unique identifier ([ProcessUUID](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processuuid))
 - `user` is the user who started the process
 - `session` is the DAQ session that is associated with this process. There may be some processes that are not associated with any session (or associated with all the sessions?) hence this is an optional field.
 - `name` is a "friendly name" that can be used to query the process (for example in a [ProcessQuery](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processquery))

```
message ProcessMetadata {
  ProcessUUID uuid = 1;
  string user = 2;
  optional string session = 3;
  string name = 4;
}
```

### `ProcessQuery`
This message can be used to query the processes run by the process manager. ProcessQuery can correspond to one or more processes (or even zero). Generally, an `OR` is formed between all the field by the process manager to get the corresponding processes.
 - `uuids` is a vector of [ProcessUUID](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processuuid) to directly get process by UUIDs.
 - `names` is a vector of processes friendly names.
 - `user` is the user
 - `session`: the session associated with the process.

```
message ProcessQuery {
  repeated ProcessUUID uuids = 1;
  repeated string names = 2;
  string user = 3;
  string session = 4;
}
```

### `ProcessDescription`
A ProcessDescription carries all the information necessary to start a process on any host. It uses two sub-message types, the StringList and ExecAndArgs. The fields mean:
 - `metadata`: to carry the [ProcessMetadata](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processmetadata) of the process
 - `env`: this is a key: value map (string to string) that stores the environment variables that need to be set for the process to run.
 - `executable_and_arguments` is a vector of executable and argument in the [ExecAndArgs](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#execandargs) format. Multiple executables can be specified and executed sequentially.
 - `process_execution_directory` (new after 27th Sept 2023) is the place where the process should be executed.
 - `process_logs_path` (new after 27th Sept 2023) is the place where the log will be stored.

```
message ProcessDescription {
  // omitting subtypes
  ProcessMetadata metadata = 1;
  map<string,string> env = 2;
  repeated ExecAndArgs executable_and_arguments = 3;
  string process_execution_directory = 4;
  string process_logs_path = 5;
}
```

### `StringList`
... is an unused vector of string. This should be deleted/ignored.

### `ExecAndArgs`
This message represents an executable and its arguments.

```
message ExecAndArgs{
  string exec = 1;
  repeated string args = 2;
};
```

### `ProcessInstance`
This message carries the description of a running process, and, eventually, the exit code.
 - `process_description` carries the [ProcessDescription](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processdescription) of the process, hence information like which executable, arguments, environment, etc.
 - `process_restriction` carries information about the host on which the process is running (well, really just the name of the host for now).
 - `status_code` is a very simple enum that show if a process is running or not (0: Running, 1: Dead).
 - `return_code` is the return code of the process.
 - `uuid` is the process unique identifier [ProcessUUID](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processuuid)

```
message ProcessInstance {
  ProcessDescription process_description = 1;
  ProcessRestriction process_restriction = 2;
  enum StatusCode {
    RUNNING = 0;
    DEAD = 1;
  };
  StatusCode status_code = 3;
  int32 return_code = 4;
  ProcessUUID uuid = 5;
}
```

### `BootRequest`
Describes requests to start a process.
 - `process_description` is used to start the process, it uses the [ProcessDescription](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processdescription) format.
 - `process_restriction` is used to choose the host on which the process will run. This follows the [ProcessRestriction](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processrestriction).

```
message BootRequest {
  ProcessDescription process_description = 1;
  ProcessRestriction process_restriction = 2;
}
```

### `ProcessInstanceList`
A list of [ProcessInstance](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processinstance).

```
message ProcessInstanceList{
  repeated ProcessInstance values = 1;
}
```

### `CommandNotificationMessage`
This is not used and should be deleted.

```
message CommandNotificationMessage {
  string user = 1;
  string command = 2;
}
```

### `GenericNotificationMessage`
This is not used and should be deleted.

```
message GenericNotificationMessage {
  string message = 1;
}
```

### `ExceptionNotification`
This is not used. The idea was to detect process exceptions, serialise them and pass them back to the client. I am not even sure this is possible so this can ignored for now.

```
message ExceptionNotification {
  message StackLine{
    string line_text = 1;
    string line_number = 2;
    string file = 3;
  };
  string error_text = 1;
  repeated StackLine stack_trace = 2;
}
```

## Endpoints
Each RPC call is described here.

### `describe`
... is already described in [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Interfacing-with-drunc).

### `boot`
Is used to boot processes. No resolution whatsoever is done for the executable arguments, they need to be formatted correctly by the client.
 - input: [BootRequest](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#bootrequest)
 - output: [ProcessInstanceList](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processinstancelist) of all the process started
 - interrupts/exceptions:
   - `allowed_hosts` is empty

For the SSH process manager, the `allowed_host_types` are not allowed in the [ProcessRestriction](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processrestriction). The process manager will try each host listed in the `allowed_hosts` until it one leads to successful execution of the process (which can itself fail).

### `restart`
Restart one (and only one) process. If the process is running, it will be killed and restarted. If the process is already dead, it simply boots it.
 - input: [ProcessQuery](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processquery)
 - output: [ProcessInstanceList](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processinstancelist) containing the one process that was started (should be `ProcessInstance` instead).
 - interrupts/exceptions:
   - The input process query leads to more than one processes, or zero process.

### `kill`
Kill one or more processes.
 - input: [ProcessQuery](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processquery)
 - output: [ProcessInstanceList](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processinstancelist) containing the processes that were killed.
 - interrupts/exceptions:
   - None known.

### `flush`
Remove the **dead** processes from the process manager. One cannot restart processes that have been flushed, and they will not appear in the [ps](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#ps).
 - input: [ProcessQuery](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processquery)
 - output: [ProcessInstanceList](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processinstancelist) containing the processes that were flushed.
 - interrupts/exceptions:
   - None known.

### `ps`
List the processes
 - input: [ProcessQuery](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processquery)
 - output: [ProcessInstanceList](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#processinstancelist) containing the processes queried.
 - interrupts/exceptions:
   - None known.

### `logs`
Stream the logs from a specific process
 - input: [LogRequest](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#logrequest)
 - output (streamed): [LogLine](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface#logline)
 - interrupt/exceptions:
   - The input process query leads to more than one processes, or zero process.