# `drunc-controller` interface

## Messages
Remember that all the messages sent to and received from the controller follow the `drunc` schema described [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Messaging-format). All the messages described here will end up in some form or another in the `data` fields of the `Request` and `Response` after being `Any`fied.

### `Status`
This message encodes the status (and FSM status) of a controller or DAQ application. It has the [form](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/controller.proto#L84C1-L90C2):
```
message Status {
  string name = 1;
  string state = 2;
  string sub_state = 3;
  bool   in_error = 4;
  bool   included = 5;
}
```
Most of the fields should be self-explanatory. The state refers to the FSM state. There is another (secondary) FSM that encodes transitions themselves. Generally, if the endpoint is not transitioning, you should get `state` = `sub_state` or `sub_state` = `idle` for a DAQ application, otherwise, you will get substate of the form `preparing-configure` etc.

### `ChildrenStatus`
This message is a list of the above Status, and encodes the statuses of the children of the controller. [Schema](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/controller.proto#L92) is:
```
message ChildrenStatus {
  repeated Status children_status = 1;
}
```

### `Argument`
This message is for describing a generic FSM argument. It has the [form](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/controller.proto#L46):
```
message Argument {
  enum Presence{
    MANDATORY = 0;
    OPTIONAL = 1;
  }
  enum Type {
    INT = 0;
    FLOAT = 1;
    STRING = 2;
    BOOL = 3;
  }
  string name = 1;
  Presence presence = 2;
  Type type = 3;
  optional google.protobuf.Any default_value = 4;
  repeated google.protobuf.Any choices = 5;
  string help = 6;
}
```
Where:
 - `name` is the name of the arguments
 - `presence` is encoded in an enum defined in the Argument schema.
 - `type` is the data type. Note that the only supported data types are the ones listed here.
 - `default_value` is the default value that the argument will take (required if presence=optional)
 - `choices` are the eventual choices that the argument can take
 - `help` is some string for help.

### `FSMCommandDescription`
have the [form](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/controller.proto#L66):
```
message FSMCommandDescription {
  string name = 1;
  repeated string data_type = 2;
  string help = 3;
  string return_type = 4;
  repeated Argument arguments = 5;
}
```
Where:
 - `name` is the name of the FSM command (such as `conf`, `enable_triggers`, etc.)
 - `data_type` is what is needed to run the command (it's always `FSMCommand` - described later)
 - `help` some explanatory text of the command (never really filled but would be nice to have at some point)
 - `return_type` is what is returned by the command (it's always `FSMCommandResponse` - described later)
 - `arguments` is a list of the arguments in [`Argument` format](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Controller-interface#argument) that are needed to execute the command.

### `FSMCommandsDescription`
Is a list of available FSM commands described above:
```
message FSMCommandsDescription {
  string type = 1;
  string name = 2;
  optional string session = 3;
  repeated FSMCommandDescription commands = 4;
}
```
Everything can be ignored except `commands` which is a list of the command accessible now. TODO: Needs cleanup

### `FSMCommand`
Have the [format](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/controller.proto#L24):
```
message FSMCommand{
  string command_name = 1;
  map<string, google.protobuf.Any> arguments = 2;
  repeated string children_nodes = 3;
  optional string data = 4; // unfortunately, this is just some plain old json data introduced by the fsm interfaces
}
```

where:
 - `command_name` is the name of the command one wants to execute (such as `conf`, `enable_triggers`, etc.)
 - `arguments` is a map of argument names to their values. Their value must be one of the 4:
    - [`int_msg`](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/generic.proto#L22)
    - [`float_msg`](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/generic.proto#L25)
    - [`string_msg`](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/generic.proto#L28)
    - [`bool_msg`](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/generic.proto#L31)
 - `children_nodes` is used to address the command to one or more children. If one wants to send a command to a specific list of children they can be specified as a list here. If no list is specified, the controller will send the FSM command to all the children.
 - `data` is the internal drunc message that gets derived from the FSMInterfaces. The user should not fill this out manually.

### `FSMResponseFlag`
Is a simple enum to understand if the command was successful or not:
```
enum FSMResponseFlag {
  FSM_EXECUTED_SUCCESSFULLY = 0;
  FSM_FAILED = 1;
  FSM_INVALID_TRANSITION = 2;
  FSM_NOT_EXECUTED_EXCLUDED = 3;
}
```

### `FSMCommandResponse`
The format is described in [here](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/controller.proto):
```
message FSMCommandResponse{
  FSMResponseFlag flag = 1;
  string command_name = 2;
  google.protobuf.Any data = 3;
}
```
where:
 - `flag` is a simple enum describing the FSM command's successful execution or not
 - `command_name` is the command that was executed.
 - `data` is the returned data for the FSM transition (which can be safely ignored for now, since it's not used)


The controller is interfaced through `gRPC` messaging. The `drunc` server should be interfaced through the `drunc`-defined [messages](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Messaging-format). This document refers to the `controller` in particular. Its `proto` can be found [here](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/controller.proto)

## Endpoints
Each `gRPC` call is described here. Note that these are commands that the `controller` endpoint uses, and are not available in `drunc-controller-shell`.

### `describe`
Describes the available endpoint commands.

### `describe_fsm`
One can ask the controller to specifically describe its FSM. The controller will answer with data of the format [FSMCommandsDescription](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Controller-interface#fsmcommandsdescription). This message will contain a list of [FSMCommandDescription](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Controller-interface#fsmcommanddescription). These commands can contain a list of [Arguments](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Controller-interface#arguments) that need to be specified for some transitions.

### `execute_fsm_command`
FSM commands can be sent to the controller by using the RPC endpoint `execute_fsm_command`:
 - input: [FSMCommand](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Controller-interface#fsmcommand)
 - output: [FSMCommandResponse](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Controller-interface#fsmcommandresponse)
 - interruption/exceptions:
    - The sender is not in control of the controller
    - The arguments in the input data are not of correct type
    - The mandatory arguments were not specified

### `get_status`
 - input: None
 - output: [Status message](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Controller-interface#status)
 - interruption/exceptions:
    - None hopefully

### `get_children_status`
 - input: None
 - output: [ChildrenStatus message](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Controller-interface#childrenstatus)
 - interruption/exceptions:
    - One child cannot be reached (I think)

### `ls`
 - input: None
 - output: [PlainTextVector](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/generic.proto#L11) containing all a list of the controller's children
 - interruption/exceptions:
    - None hopefully

### `exclude`
Excluded nodes won't be passed FSM commands. This command excludes the controller and all its descendants.
 - input: None (TODO should be a list of children to exclude)
 - output: [PlainText](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/generic.proto#L7) of the form: "<controller_name> excluded"
 - interruption/exceptions:
    - The sender is not in control of the controller
    - The controller is already excluded

### `include`
Excluded nodes won't be passed FSM commands. This command includes the controller and all its descendants.
 - input: None (TODO should be a list of children to include)
 - output: [PlainText](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/generic.proto#L7) of the form: "<controller_name> included"
 - interruption/exceptions:
    - The sender is not in control of the controller
    - The controller is already included

### `take_control`
Takes control of a controller and it's children
 - input: None (TODO should be a list of children to take control of)
 - output: [PlainText](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/generic.proto#L7) of the form: "<user_name> took control"
 - interruption/exceptions:
    - Someone is already in control of the controller (including the same user name)

### `surrender_control`
Surrenders control of a controller and it's children
 - input: None (TODO should be a list of children to surrender the control of)
 - output: [PlainText](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/generic.proto#L7) of the form: "<user_name> surrendered control"
 - interruption/exceptions:
    - The sender is not in control of the controller

### `who_is_in_charge`
Says who is controlling the controller
 - input: None
 - output: [PlainText](https://github.com/DUNE-DAQ/druncschema/blob/develop/schema/druncschema/generic.proto#L7) of the form: "<user_name>"
 - interruption/exceptions:
    - None hopefully.
