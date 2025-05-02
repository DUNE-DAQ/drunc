# Unified Shell Reference
All the commands that are issued in the shell take a `--help` flag, you can also use tab completion. Note the examples use the topology:

```txt
drunc-unified-shell > status
                                       your-choice-of-session-name status
┏━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Name                   ┃ Info      ┃ State   ┃ Substate ┃ In error ┃ Included ┃ Endpoint                  ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ root-controller        │           │ initial │ initial  │ No       │ Yes      │ grpc://10.73.136.38:43493 │
│   df-controller        │           │ initial │ initial  │ No       │ Yes      │ grpc://10.73.136.38:34661 │
│     df-01              │           │ initial │ idle     │ No       │ Yes      │ rest://10.73.136.38:50703 │
│     dfo-01             │           │ initial │ idle     │ No       │ Yes      │ rest://10.73.136.38:36819 │
│     tp-stream-writer   │           │ initial │ idle     │ No       │ Yes      │ rest://10.73.136.38:58985 │
│   hsi-fake-controller  │           │ initial │ initial  │ No       │ Yes      │ grpc://10.73.136.38:36509 │
│     hsi-fake-01        │           │ initial │ idle     │ No       │ Yes      │ rest://10.73.136.38:46847 │
│     hsi-fake-to-tc-app │           │ initial │ idle     │ No       │ Yes      │ rest://10.73.136.38:44491 │
│   ru-controller        │           │ initial │ initial  │ No       │ Yes      │ grpc://10.73.136.38:38695 │
│     ru-01              │ conn apa1 │ initial │ idle     │ No       │ Yes      │ rest://10.73.136.38:46305 │
│   trg-controller       │           │ initial │ initial  │ No       │ Yes      │ grpc://10.73.136.38:35241 │
│     mlt                │           │ initial │ idle     │ No       │ Yes      │ rest://10.73.136.38:53607 │
│     tc-maker-1         │           │ initial │ idle     │ No       │ Yes      │ rest://10.73.136.38:40777 │
└────────────────────────┴───────────┴─────────┴──────────┴──────────┴──────────┴───────────────────────────┘
```

## boot
### Description
This command spawns the processes that are used by the DAQ. In most cases, it SSHes on the host where the process is supposed to run, and execute the `daq_application` or `drunc-controller` binary.

The `boot` command will check if there are processes running in the process manager with the same session name and ask for confirmation if it detects other process running under the same session name.

The `boot` command can take the following option:
* `--override-logs/--no-override-logs` (optional), this flags adds a timestamp to the log files of the application, effectively making them non-overriding. Note this happens only in the case where the `log_path` is _not_ set in your configuration's `Session` or `Application` objects. If the configuration's `log_path` is not `./` in either of these, the run control will use that, and the log will not be overriding (in this case, _this flag is ignored_).

### Example
```bash
boot --override-logs
```

### Related commands
* [`kill`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#kill)
* [`ps`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#ps)

## change-rate
### Description
**NOTE**: This command depends on your FSM configuration. If you don't use the standard FSM from the DAQ, you may not have it!

**NOTE**: This command requires you to have booted the system and to be connected to a controller.

This command allows you to change the trigger rate of the system, when it is `running` or `ready`, this command does not change the state of the system.

The `change-rate` command can take the following options:
* `--trigger-rate` (mandatory), is a float to specify the trigger rate in Hz you want the system to be running at.
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment and children if you specify it. By default, the target is the top segment for the whole session.

### Examples
To send `change-rate` to the whole of the DAQ:
```bash
change-rate --trigger-rate 0.01
```
To send `change-rate` to the whole trigger segment:
```bash
change-rate --trigger-rate 0.01 --target root-controller/trg-controller
```
To send `change-rate` to the MLT only:
```bash
change-rate --trigger-rate 0.01 --target root-controller/trg-controller/mlt
```

### Related commands
* [`start`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#start)
* [`enable-triggers`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#enable-triggers)
* [`disable-triggers`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#disable-triggers)
* [`drain-dataflow`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#drain-dataflow)

## conf
### Description
**NOTE**: This command depends on your FSM configuration. If you don't use the standard FSM from the DAQ, you may not have it!

**NOTE**: This command requires you to have booted the system and to be connected to a controller.

This command allows you to configure the system, when it is `initialised`, and thus to reach the `configured` state.

The `conf` command can take the following option:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment and children if you specify it. By default, the target is the top segment for the whole session.

### Examples
To send `conf` to the whole of the DAQ:
```bash
conf
```
To send `conf` to the whole trigger segment:
```bash
conf --target root-controller/trg-controller
```
To send `conf` to the MLT only:
```bash
conf --target root-controller/trg-controller/mlt
```

### Related commands
* [`scrap`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#scrap)
* [`start`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#start)

## connect
### Description
This command is used to connect the shell to a controller.

The `connect` command takes the control address of the controller as argument

If you are already connected to a controller, you will be asked to confirm that you want to connect to another controller.

The `connect` command take the following options:
* `-f/--force`, which can be used to skip the confirmation step in case you are already connected to another controller.

### Example
```bash
connect grpc://np04-srv-019:56582
```

### Related command
* [`disconnect`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#disconnect)

## disable-triggers
### Description
**NOTE**: This command depends on your FSM configuration. If you don't use the standard FSM from the DAQ, you may not have it!

**NOTE**: This command requires you to have booted the system and to be connected to a controller.

This command allows you to disable the triggers of the system, when it is `running`, and thus to reach the `ready` state.

The `disable-triggers` command can take the following option:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment and children if you specify it. By default, the target is the top segment for the whole session.

### Examples
To send `disable-triggers` to the whole of the DAQ:
```bash
disable-triggers
```
To send `disable-triggers` to the whole trigger segment:
```bash
disable-triggers --target root-controller/trg-controller
```
To send `disable-triggers` to the MLT only:
```bash
disable-triggers --target root-controller/trg-controller/mlt
```

### Related commands
* [`enable-triggers`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#enable-triggers)
* [`change-rate`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#change-rate)

## disconnect
### Description
This command is used to disconnect the shell to a controller.

The `disconnect` command takes the control address of the controller as argument

If you are connected to a controller, you will be asked to confirm that you want to disconnect.

The `disconnect` command take the following options:
* `-f/--force`, which can be used to skip the confirmation step in case you are already connected to another controller.

### Example
```bash
disconnect
```

### Related command
* [`connect`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#connect)

## drain-dataflow
### Description
**NOTE**: This command depends on your FSM configuration. If you don't use the standard FSM from the DAQ, you may not have it!

**NOTE**: This command requires you to have booted the system and to be connected to a controller.

This command allows you to drain the dataflow of the system, when it is `ready`, and thus to reach the `dataflow-drained` state.

The `drain-dataflow` command can take the following options, depending on your FSM configuration:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment and children if you specify it. By default, the target is the top segment for the whole session.
* `--elisa-post` (optional), allows you to add a message on the ELisA logbook post that gets created when the run finishes.
* `--file-logbook-post` (optional), allows you to add a message on the file logbook post that gets create when the run finishes.
### Examples
To send `drain-dataflow` to the whole of the DAQ:
```bash
drain-dataflow
```
To send `drain-dataflow` to the whole trigger segment:
```bash
drain-dataflow --target root-controller/trg-controller
```
To send `drain-dataflow` to the MLT only:
```bash
drain-dataflow --target root-controller/trg-controller/mlt
```

### Related commands
* [`start`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#start)
* [`disable-triggers`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#disable-triggers)
* [`stop-trigger-sources`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#stop-trigger-sources)

## enable-triggers
### Description
**NOTE**: This command depends on your FSM configuration. If you don't use the standard FSM from the DAQ, you may not have it!

**NOTE**: This command requires you to have booted the system and to be connected to a controller.

This command allows you to enable the trigger of the system, when it is `ready`, and thus to reach the `running` state.

The `enable-triggers` command can take the following options:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment and children if you specify it. By default, the target is the top segment for the whole session.

### Examples
To send `enable-triggers` to the whole of the DAQ:
```bash
enable-triggers
```
To send `enable-triggers` to the whole trigger segment:
```bash
enable-triggers --target root-controller/trg-controller
```
To send `enable-triggers` to the MLT only:
```bash
enable-triggers --target root-controller/trg-controller/mlt
```

### Related commands
* [`start`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#start)
* [`disable-triggers`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#disable-triggers)

## exclude
### Description
**NOTE**: This command requires you to have booted the system and to be connected to a controller.

Allows you to exclude a segment or an application from the DAQ system. All commands to the excluded part of the system are then ignored, and the next time `recompute-status` is issued, excluded nodes states are ignored.

The `exclude` command can take the following options:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment if you specify it. By default, the target is the top segment for the whole session.

### Examples
To `exclude` to the whole trigger segment:
```bash
exclude --target root-controller/trg-controller
```
To `exclude` to the MLT only:
```bash
exclude --target root-controller/trg-controller/mlt
```

### Related commands
* [`include`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#include)
* [`status`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#status)
* [`recompute-status`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#recompute-status)

## expert-command
### Description
**NOTE**: This command requires you to have booted the system and to be connected to a controller.

Send JSON to a DAQ application. Excluded nodes states are ignored.

The format of the JSON is:
```json
{
    "data": {
        "modules": [
            {
                "data": {
                    "CHANGE THAT": "CHANGE THAT"
                },
                "match": "CHANGE THAT"
            }
        ]
    },
    "entry_state": "CHANGE THAT",
    "exit_state": "CHANGE THAT",
    "id": "CHANGE THAT"
}
```
Where:
* `entry_state` and `exit_state` need to be set to _the same state in the FSM_, written in capital.
* `id` is the command name the application will respond to (`register_command` in your module).
* `data.modules[:].data` is the data you are sending to the modules.
* `data.modules[:].match` is the name of the module, if left blank (""), the command will be propagated to all the module that have registered the command `id`.

`expert-command` command take the following mandatory argument:
* `command`, which is either the path to a json file that will be sent to the application(s) or, an actual json string (wrapped in `"`). An example of the file can be found in [here](https://github.com/DUNE-DAQ/drunc/blob/develop/src/drunc/data/expert-command/record.json).

The `expert-command` command can take the following options:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment if you specify it. By default, the target is the top segment for the whole session.
* `-s/--string` (optional), a flag to signify that you are sending raw json, rather than a json file.

### Examples
To send an expert command to the whole readout segment application:
```bash
expert-command --target root-controller/ru-controller data/record.json
```


## flush
### Description
This command removes the dead processes from the `ps` list, after issuing `flush`, you will not be able to send `restart` to that process.

The `flush` command can take the following options:
* `--uuid`, to select a process to flush based on its UUID.
* `-u/--user`, to select the processes to flush based on its user.
* `-n/--name`, to select a process to flush based on its "friendly name".
* `-s/--session`, to select the processes to flush based on a session name.

By default, `flush` will flush all the dead processes from all sessions, users and names.

All the processes metadata can be which can be found by issuing `ps`.

### Examples
To `flush` all the dead processes:
```bash
flush
```
To `flush` the MLT:
```bash
flush --name mlt
```
To `flush` the whole session:
```bash
flush -s your-session-name
```

### Related commands
* [`kill`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#kill)
* [`ps`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#ps)

## include
### Description
**NOTE**: This command requires you to have booted the system and to be connected to a controller.

Allows you to include a segment or an application (that was previously excluded) from the DAQ system.

The `include` command can take the following options:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment if you specify it. By default, the target is the top segment for the whole session.

### Examples
To `include` to the whole trigger segment:
```bash
include --target root-controller/trg-controller
```
To `include` to the MLT only:
```bash
include --target root-controller/trg-controller/mlt
```

### Related commands
* [`exclude`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#exclude)
* [`status`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#status)
* [`recompute-status`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#recompute-status)

## kill
### Description
This command kills running processes.

The `kill` command must take at least one the following options:
* `--uuid`, to select a process to flush based on its UUID.
* `-u/--user`, to select the processes to flush based on its user.
* `-n/--name`, to select a process to flush based on its "friendly name".
* `-s/--session`, to select the processes to flush based on a session name.

By default, `kill` does nothing, you need to supply one of the options.

All the processes metadata can be which can be found by issuing `ps`.

### Examples
To `kill` the MLT:
```bash
kill --name mlt
                                                  Killed process
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━┓
┃ session      ┃ friendly name ┃ user     ┃ host      ┃ uuid                                 ┃ alive ┃ exit-code ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━┩
│ test-session │     mlt       │ pplesnia │ localhost │ e1f45c8c-1505-46b9-bb4f-a05b138ea314 │ False │ 255       │
└──────────────┴───────────────┴──────────┴───────────┴──────────────────────────────────────┴───────┴───────────┘
```
To `kill` the whole session:
```bash
kill -s your-session-name
```

### Related commands
* [`terminate`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#terminate)
* [`ps`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#ps)
* [`boot`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#boot)
* [`restart`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#restart)


## logs
### Description
This command prints the log of a processes.

The `logs` command must take at least one the following options:
* `--uuid`, to select a process to flush based on its UUID.
* `-u/--user`, to select the processes to flush based on its user.
* `-n/--name`, to select a process to flush based on its "friendly name".
* `-s/--session`, to select the processes to flush based on a session name.
* `--how-far`, how many lines to print, by default it `logs` print the last 100 lines of log.
* `--grep`, to select a particular string in the logs. Works with regex too.

By default, `logs` does nothing, you need to supply one or more of the options `--uuid`, `-u/--user`, `-n/--name`, `-s/-session`, and the logical _AND_ of this options must correspond to exactly one process.

All the processes metadata can be which can be found by issuing `ps`.

### Example
To get the `logs` of the root-controller:
```bash
logs -n root-controller --how-far 5
────────────────────────────────────────────────────────────────────────── a61ffe46-dfa2-4a90-b888-7901fa5755b2 logs ───────────────────────────────────────────────────────────────────────────
           INFO     "Controller": 'df-controller@localhost:5600' (type ChildNodeType.gRPC)                                           controller.py:123
           INFO     "Controller": 'trg-controller@localhost:5700' (type ChildNodeType.gRPC)                                          controller.py:123
           INFO     "Controller": 'hsi-controller@localhost:5800' (type ChildNodeType.gRPC)                                          controller.py:123
           INFO     "Broadcast": ready                                                                                          broadcast_sender.py:65
root-controller was started on localhost:3333
───────────────────────────────────────────────────────────────────────────────────────────── End ──────────────────────────────────────────────────────────────────────────────────────────────
```
To get the errors in `logs` of the MLT:
```bash
logs --name mlt --grep ERROR
```

### Related command
* [`ps`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#ps)


## ps
### Description
This command list running processes.

The `ps` command must take at least one the following options:
* `--uuid`, to select a process to flush based on its UUID.
* `-u/--user`, to select the processes to flush based on its user.
* `-n/--name`, to select a process to flush based on its "friendly name".
* `-s/--session`, to select the processes to flush based on a session name.
* `--long-format/-l`, to get a long listing format.

By default, `ps` list all the processes.

### Examples
To check the state the MLT process:
```bash
ps --name mlt
```
To check the state of the whole session:
```bash
ps -s your-session-name
```
To check the state of all the processes:
```bash
ps
                                                      Processes running
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━┓
┃ session      ┃ friendly name             ┃ user     ┃ host      ┃ uuid                                 ┃ alive ┃ exit-code ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━┩
│ test-session │ root-controller           │ pplesnia │ localhost │ a61ffe46-dfa2-4a90-b888-7901fa5755b2 │ True  │ 0         │
│ test-session │   local-connection-server │ pplesnia │ localhost │ 1285a63b-637b-4ac8-a30a-62cd419505bc │ True  │ 0         │
│ test-session │   ru-controller           │ pplesnia │ localhost │ d8541d0a-8c21-416f-870b-1dc0b0180857 │ True  │ 0         │
│ test-session │     ru-01                 │ pplesnia │ localhost │ 5f75d23f-ee0a-4fa5-adfa-151ae8336683 │ True  │ 0         │
│ test-session │   df-controller           │ pplesnia │ localhost │ 0710d816-6a21-4501-b9ab-afb4bba08c23 │ True  │ 0         │
│ test-session │     tp-stream-writer      │ pplesnia │ localhost │ ad84b119-595c-4489-9207-8f4929a5d53a │ True  │ 0         │
│ test-session │     dfo-01                │ pplesnia │ localhost │ fb45ef4d-a19e-433d-9b0b-35457c8666e1 │ True  │ 0         │
│ test-session │     df-01                 │ pplesnia │ localhost │ 82615d14-dca5-4180-8f40-3f17180a9d87 │ True  │ 0         │
│ test-session │     df-02                 │ pplesnia │ localhost │ eefc8fce-2b4d-4de1-9d20-1c209902f98e │ True  │ 0         │
│ test-session │   trg-controller          │ pplesnia │ localhost │ fbb8eacb-62e6-44eb-bb1c-4585cfbcd033 │ True  │ 0         │
│ test-session │     tc-maker-1            │ pplesnia │ localhost │ 9539be3d-6b51-4129-a474-12e3c268d2df │ True  │ 0         │
│ test-session │     mlt                   │ pplesnia │ localhost │ e1f45c8c-1505-46b9-bb4f-a05b138ea314 │ True  │ 0         │
│ test-session │     hsi-to-tc-app         │ pplesnia │ localhost │ ec82eee5-fed9-4906-992c-8721d3be6f7f │ True  │ 0         │
│ test-session │   hsi-controller          │ pplesnia │ localhost │ f1765e99-80bf-4a39-91f5-a56bc36962db │ True  │ 0         │
│ test-session │     hsi-01                │ pplesnia │ localhost │ cef1b4ff-6fff-4efc-a66a-08a9dce28c24 │ True  │ 0         │
└──────────────┴───────────────────────────┴──────────┴───────────┴──────────────────────────────────────┴───────┴───────────┘
```

### Related commands
* [`kill`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#kill)
* [`logs`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#logs)
* [`restart`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#restart)
* [`boot`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#boot)

## recompute-status
### Description
**NOTE**: This command requires you to have booted the system and to be connected to a controller.

Figure out the status the status of the controller, from the state of its _included_ children.

The `recompute-status` command takes the following options:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment and children if you specify it. By default, the target is the top segment for the whole session.
* `--execute-along-path/--dont-execute-along-path` (optional), the command gets executed along the path of the target, starting with the top controller, etc. By default, execute along path is true.
* `--execute-on-all-subsequent-children-in-path/--dont-execute-on-all-subsequent-children-in-path` (optional), the command gets executed on all the children (applications and controllers) that are controlled by target (similar to `-r/--recursive` for `rm` or `grep`). By default, execute on all subsequent children in path.

By default, the `recompute-status` recomputes the status of all the system.

### Example
To recompute the status of the whole system:
```bash
recompute-status
```
To recompute the status of the trg-controller, and the root-controller:
```bash
recompute-status --target root-controller/trg-controller
```
To recompute the status of the trg-controller _only_:
```bash
recompute-status --target root-controller/trg-controller --dont-execute-along-path
```

### Related commands
* [`status`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#status)
* [`exclude`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#exclude)
* [`include`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#include)


## restart
### Description
**NOTE**: This command isn't typically used right now, most likely this will not work as intended.

Restart a process that was booted.

The `restart` command must take at least one the following options:
* `--uuid`, to select a process to flush based on its UUID.
* `-u/--user`, to select the processes to flush based on its user.
* `-n/--name`, to select a process to flush based on its "friendly name".
* `-s/--session`, to select the processes to flush based on a session name.

By default, `restart` does nothing, you need to supply one or more of the options `--uuid`, `-u/--user`, `-n/--name`, `-s/-session`, and the logical _AND_ of this options must correspond to exactly one process.

### Example
To `restart` the MLT process:
```bash
restart --name mlt
```

### Related commands
* [`boot`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#boot)
* [`kill`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#kill)
* [`ps`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#ps)


## scrap
### Description
**NOTE**: This command depends on your FSM configuration. If you don't use the standard FSM from the DAQ, you may not have it!

**NOTE**: This command requires you to have booted the system and to be connected to a controller.

This command allows you to "unconfigure" the system, when it is `configured`, and thus to reach the `initial` state.

The `scrap` command can take the following option:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment and children if you specify it. By default, the target is the top segment for the whole session.

### Examples
To send `scrap` to the whole of the DAQ:
```bash
scrap
```
To send `scrap` to the whole trigger segment:
```bash
scrap --target root-controller/trg-controller
```
To send `scrap` to the MLT only:
```bash
scrap --target root-controller/trg-controller/mlt
```

### Related commands
* [`conf`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#conf)
* [`start`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#start)

## start
### Description
**NOTE**: This command depends on your FSM configuration. If you don't use the standard FSM from the DAQ, you may not have it!

**NOTE**: This command requires you to have booted the system and to be connected to a controller.

This command allows you to "start" the run on system, when it is `configured`, and thus to reach the `ready` state.

The `start` command can take the following options:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment and children if you specify it. By default, the target is the top segment for the whole session.
* `--elisa-post` (optional), allows you to add a message on the ELisA logbook post that gets created when the run ends. By default, nothing is added on the post.
* `--file-logbook-post` (optional), allows you to add a message on the file logbook post that gets create when the run ends. By default, nothing is added on the post.
* `--run-type` (optional), either _PROD_ or _TEST_, depending on how you think the data will be used. By default, _TEST_ is used.
* `--trigger-rate` (optional), the trigger rate you want to set. By default it's 0 Hz, but the system will use the trigger rate from the configuration.
* `--disable-data-storage` (optional), wether to disable the writing of the data and have a "dry run" of the DAQ. By default, write data.
* `--run-number` (required, in some cases), which run number to use.

### Examples
To send `start`:
```bash
start --run-number 1
```
To send `start` and add a message to the ELisa logbook, and run in PROD mode:
```bash
start --elisa-post "A run with the TDE and PDS" --run-type PROD
```

### Related commands
* [`conf`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#conf)
* [`enable-triggers`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#enable-triggers)
* [`drain-dataflow`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#drain-dataflow)

## status
### Description
**NOTE**: This command requires you to have booted the system and to be connected to a controller.

Prints the status the status of the controller.

The `status` command takes the following options:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment and children if you specify it. By default, the target is the top segment for the whole session.
* `--execute-along-path/--dont-execute-along-path` (optional), the command gets executed along the path of the target, starting with the top controller, etc. By default, execute along path is true.
* `--execute-on-all-subsequent-children-in-path/--dont-execute-on-all-subsequent-children-in-path` (optional), the command gets executed on all the children (applications and controllers) that are controlled by target (similar to `-r/--recursive` for `rm` or `grep`). By default, execute on all subsequent children in path.

By default, the `status` prints the status of all the system.

### Example
To get the status of the whole system:
```bash
status
```
To get the status of the trg-controller, and the root-controller:
```bash
status --target root-controller/trg-controller
```
To get the status of the trg-controller _only_:
```bash
status --target root-controller/trg-controller --dont-execute-along-path
```

### Related commands
* [`recompute-status`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#recompute-status)
* [`exclude`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#exclude)
* [`include`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#include)

## stop
### Description
**NOTE**: This command depends on your FSM configuration. If you don't use the standard FSM from the DAQ, you may not have it!

**NOTE**: This command requires you to have booted the system and to be connected to a controller.

This command allows you to stop the system, when it is in the `trigger-sources-stopped` state, and thus to reach the `configured` state.

The `stop` command can take the following option:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment and children if you specify it. By default, the target is the top segment for the whole session.

### Examples
To send `stop` to the whole of the DAQ:
```bash
stop
```
To send `stop` to the whole trigger segment:
```bash
stop --target root-controller/trg-controller
```
To send `stop` to the MLT only:
```bash
stop --target root-controller/trg-controller/mlt
```

### Related commands
* [`scrap`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#scrap)
* [`start`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#start)
* [`stop-trigger-sources`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#stop-trigger-sources)

## stop-trigger-sources
### Description
**NOTE**: This command depends on your FSM configuration. If you don't use the standard FSM from the DAQ, you may not have it!

**NOTE**: This command requires you to have booted the system and to be connected to a controller.

This command allows you to stop the trigger sources of the system, when it is in the `dataflow-drained` state, and thus to reach the `trigger-sources-stopped` state.

The `stop-trigger-sources` command can take the following option:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment and children if you specify it. By default, the target is the top segment for the whole session.

### Examples
To send `stop-trigger-sources` to the whole of the DAQ:
```bash
stop-trigger-sources
```
To send `stop-trigger-sources` to the whole trigger segment:
```bash
stop-trigger-sources --target root-controller/trg-controller
```
To send `stop-trigger-sources` to the MLT only:
```bash
stop-trigger-sources --target root-controller/trg-controller/mlt
```

### Related commands
* [`stop`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#stop)
* [`drain-dataflow`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#drain-dataflow)

## surrender-control
### Description
**NOTE**: This command requires you to have booted the system and to be connected to a controller.

**NOTE**: This command isn't typically used right now.

This commands allows you to give up the control of a controller and its children. Note the user must be in control to be able to send commands to the controller

The `surrender-control` command takes the following options:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment and children if you specify it. By default, the target is the top segment for the whole session.
* `--execute-along-path/--dont-execute-along-path` (optional), the command gets executed along the path of the target, starting with the top controller, etc. By default, execute along path is true.
* `--execute-on-all-subsequent-children-in-path/--dont-execute-on-all-subsequent-children-in-path` (optional), the command gets executed on all the children (applications and controllers) that are controlled by target (similar to `-r/--recursive` for `rm` or `grep`). By default, execute on all subsequent children in path.

By default, `surrender-control` surrenders the control on the whole system.

### Example
To give up the control of the whole session:
```bash
surrender-control
```
To give up the control of the trg-controller and its children:
```bash
surrender-control --target root-controller/trg-controller
```

### Related commands
* [`take-control`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#take-control)
* [`who-is-in-charge`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#who-is-in-charge)
* [`whoami`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#whoami)

## take-control
### Description
**NOTE**: This command requires you to have booted the system and to be connected to a controller.

**NOTE**: This command isn't typically used right now.

This commands allows you to take the control of a controller and its children. Note the user must be in control to be able to send commands to the controller

The `take-control` command takes the following options:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment and children if you specify it. By default, the target is the top segment for the whole session.
* `--execute-along-path/--dont-execute-along-path` (optional), the command gets executed along the path of the target, starting with the top controller, etc. By default, execute along path is true.
* `--execute-on-all-subsequent-children-in-path/--dont-execute-on-all-subsequent-children-in-path` (optional), the command gets executed on all the children (applications and controllers) that are controlled by target (similar to `-r/--recursive` for `rm` or `grep`). By default, execute on all subsequent children in path.

By default, `take-control` surrenders the control on the whole system.

### Example
To take control of the whole session:
```bash
take-control
```
To take control of the trg-controller and its children:
```bash
take-control --target root-controller/trg-controller
```

### Related commands
* [`surrender-control`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#surrender-control)
* [`who-is-in-charge`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#who-is-in-charge)
* [`whoami`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#whoami)

## terminate
### Description
This command kills all the running processes in your instance of the process manager.

The `terminate` takes no option.

### Example
To `terminate` all the processes:
```bash
terminate
                                                      Terminated process
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━┓
┃ session      ┃ friendly name             ┃ user     ┃ host      ┃ uuid                                 ┃ alive ┃ exit-code ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━┩
│ test-session │ root-controller           │ pplesnia │ localhost │ a61ffe46-dfa2-4a90-b888-7901fa5755b2 │ False │ 255       │
│ test-session │   local-connection-server │ pplesnia │ localhost │ 1285a63b-637b-4ac8-a30a-62cd419505bc │ False │ 255       │
│ test-session │   ru-controller           │ pplesnia │ localhost │ d8541d0a-8c21-416f-870b-1dc0b0180857 │ False │ 255       │
│ test-session │     ru-01                 │ pplesnia │ localhost │ 5f75d23f-ee0a-4fa5-adfa-151ae8336683 │ False │ 255       │
│ test-session │   df-controller           │ pplesnia │ localhost │ 0710d816-6a21-4501-b9ab-afb4bba08c23 │ False │ 255       │
│ test-session │     tp-stream-writer      │ pplesnia │ localhost │ ad84b119-595c-4489-9207-8f4929a5d53a │ False │ 255       │
│ test-session │     dfo-01                │ pplesnia │ localhost │ fb45ef4d-a19e-433d-9b0b-35457c8666e1 │ False │ 255       │
│ test-session │     df-01                 │ pplesnia │ localhost │ 82615d14-dca5-4180-8f40-3f17180a9d87 │ False │ 255       │
│ test-session │     df-02                 │ pplesnia │ localhost │ eefc8fce-2b4d-4de1-9d20-1c209902f98e │ False │ 255       │
│ test-session │   trg-controller          │ pplesnia │ localhost │ fbb8eacb-62e6-44eb-bb1c-4585cfbcd033 │ False │ 255       │
│ test-session │     tc-maker-1            │ pplesnia │ localhost │ 9539be3d-6b51-4129-a474-12e3c268d2df │ False │ 255       │
│ test-session │   hsi-controller          │ pplesnia │ localhost │ f1765e99-80bf-4a39-91f5-a56bc36962db │ False │ 255       │
│ test-session │     hsi-01                │ pplesnia │ localhost │ cef1b4ff-6fff-4efc-a66a-08a9dce28c24 │ False │ 255       │
└──────────────┴───────────────────────────┴──────────┴───────────┴──────────────────────────────────────┴───────┴───────────┘
```

### Related commands
* [`kill`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#kill)
* [`ps`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#ps)
* [`boot`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#boot)
* [`restart`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#restart)

## wait
### Description
This command make the run control wait.

The `wait` takes one argument, the number of second to wait

### Example
To `wait` 10 seconds:
```bash
wait 10
```

## who-is-in-charge
### Description
**NOTE**: This command requires you to have booted the system and to be connected to a controller.

**NOTE**: This command isn't typically used right now.

**NOTE**: This command does not render correctly right now who is in charge for the children.

This commands displays who is has the control of a controller and its children. Note the user must be in control to be able to send commands to the controller

The `who-is-in-charge` command takes the following options:
* `--target` (optional), allows you to specify a target application/segment for the command. The command will only be run on that application or segment and children if you specify it. By default, the target is the top segment for the whole session.
* `--execute-along-path/--dont-execute-along-path` (optional), the command gets executed along the path of the target, starting with the top controller, etc. By default, execute along path is true.
* `--execute-on-all-subsequent-children-in-path/--dont-execute-on-all-subsequent-children-in-path` (optional), the command gets executed on all the children (applications and controllers) that are controlled by target (similar to `-r/--recursive` for `rm` or `grep`). By default, execute on all subsequent children in path.

By default, `who-is-in-charge` surrenders the control on the whole system.

### Example
To see who is in charge of the whole session:
```bash
who-is-in-charge
```
To see who is in charge of the trg-controller and its children:
```bash
who-is-in-charge --target root-controller/trg-controller
```

### Related commands
* [`take-control`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#take-control)
* [`surrender-control`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#surrender-control)
* [`whoami`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#whoami)

## whoami### Description
**NOTE**: This command isn't typically used right now.

This command prints your name.

### Example
To print your name:
```bash
whoami
> jcvandamme
