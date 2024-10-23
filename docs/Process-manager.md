# `drunc-process_manager`
This app is responsible for the `drunc` processes, with the available process commands defined in [available commands](#Available-commands). If you are running with `drunc-unified-shell`, both the `proces_manager` and `process-manager-shell` outputs will be dumped to the `drunc-unified-shell` `stdout`.
For a standalone `process_manager` you will need two shells - one shell to run the `process_manager`, and the other will interact with the `process_manager` through a `process-manager-shell`

## Configurations
To boot a `process_manager`, you will need to choose the most appropriate configuration that applies to the use case. The configurations that are packaged with `drunc` are defined in `drunc/src/data/process_manager/`, which are
 - `ssh-standalone.json` - `ssh` based standalone implementation without a `kafka` feed.
 - `ssh-kafka.json` - `ssh` based implementation with `kafka` for message broadcasting.
 - `ssh-CERN-kafka.json` - `ssh` based implementation with `kafka` service running at ENH1.
 - `k8s.json` - `kubernetes` implementation (not recommended nor working, so don't use this unless you are an working on getting it to work).

This is also the appropriate place to define new `process_manager` configurations should they be necessary.

## Run a standalone `process_manager`
Note that this runs the process manager daemon, _you will not be able to do anything else with it other than starting it and ctrl-c it_.

This goes as
```
drunc-process-manager <configuration_file>
```

To start the ssh version without kafka:
```bash
(dbt) [pplesnia@np04-srv-019 drunc]$ drunc-process-manager ssh-standalone
Using 'file://src/drunc/data/process_manager/ssh-standalone.json' as the ProcessManager configuration
Starting 'SSHProcessManager'
[12:43:26] INFO     "BroadcastSenderConfHandler": None                                                                                                                                                  configuration.py:25
           INFO     "Controller": DummyAuthoriser ready                                                                                                                                              dummy_authoriser.py:13
ProcessManager was started on np04-srv-019:10054
```
Once this is done, you will not be able to send commands to the process from the current shell with the `process_manager` acting in the foreground. To interact with a standalone instance of `process_manager` you will need to connect to it (see below).

## Connect to the `process_manager`
This is done directly with the address of the process manager in a separate shell. When spawning a standalone instance, the port to connect through is printed in the last line. Communication is done using `gRPC`. The connection command is
```bash
drunc-process-manager-shell grpc://<hostname>:<port>
```
For the example spawned above, in the `process-manager-shell` this is
```bash
drunc-process-manager-shell grpc://localhost:10054
```

## Available commands
Let's start the `daqsystemtest` example `local-1x1-config` session defined [in this file](https://github.com/DUNE-DAQ/daqsystemtest/blob/develop/config/daqsystemtest/example-configs.data.xml)

### `boot`
Spawns the DAQ system processes defined in a system configuration file.
Required arguments (in this order):
 - `boot_configuration`: path to the system configuration file (written in `OKS`).
 - `session_name`: name of the session defined in the system configuration file.

Optional arguments:
 - `--no-override-logs/--override-logs`: decides whether to override the logs, if `--override-logs` is used the log filenames will not include a timestamp, otherwise if `--no-overrride-logs` is the log filenames will include a timestamp. By default, the logs are overwritten.
 - `-l/--log-level`: sets the log level.
 - `-u/--user`: assigns an owner to the spawned processes, default is `$USER`.

Caveats:
 - It is most likely impossible to specify a `user` different from the one that is running the `process_manager`, simply because that user will likely not have the ssh keys necessary to ssh on a different host as a different user.

Example:
We now boot the session and get, in the `process-manager-shell`:
```bash
drunc-process-manager > boot config/daqsystemtest/example-configs.data.xml local-1x1-config
test/config/test-session.data.xml
[14:10:01] INFO     "_convert_oks_to_boot_request":                                                                                                                 process_manager_driver.py:35
                    /cvmfs/dunedaq-development.opensciencegrid.org/[...]/config/daqsystemtest/example-configs.data.xml
           INFO     "collect_apps": Ignoring disabled app ru-02                                                                                                                 oks_parser.py:95
           INFO     "process_manager_driver": RTE script was not supplied in the OKS configuration, using the one from local enviroment instead                     process_manager_driver.py:82
'root-controller' (a61ffe46-dfa2-4a90-b888-7901fa5755b2) process started

[...many more process booted...]
'local-connection-server' (1285a63b-637b-4ac8-a30a-62cd419505bc) process started
                                                ╭─────────────────────────────────────────────────────────────────────────────────────────────╮
                                                │                                                                                             │
                                                │                                                                                             │
                                                │      Controller endpoint: 'localhost:3333', point your 'drunc-controller-shell' to it.      │
                                                │                                                                                             │
                                                │                                                                                             │
                                                ╰─────────────────────────────────────────────────────────────────────────────────────────────╯
```

On the other side, the porcess manager daemon said:
```bash
[14:10:01] INFO     "ssh-process-manager": Booting user: "pplesnia"                                                                                                   ssh_process_manager.py:220
                    session: "local-1x1-config"
                    name: "root-controller"
                    tree_id: "1.0.0"

           INFO     "ssh-process-manager": Booted root-controller uid: a61ffe46-dfa2-4a90-b888-7901fa5755b2
           INFO     "process_manager_driver": RTE script was not supplied in the OKS configuration, using the one from local enviroment instead                     process_manager_driver.py:82
```

### `ps`
Lists running processes.

There are no mandatory arguments. This command will by default list all the processes from any session, with any name, spawned by any user, and with any UUID as long as its under the management of the current instance of `process_manger`.

Options are:
 - `--long-format/-l`: get a long listing format.
 - `--session/-s`: list processes from a specific session.
 - `--name/-n`: list processes with a specific name.
 - `--user/-u`: list processes from a specific user.
 - `--uuid`: list processes with a specific unique identifier.

All options aside from `--long-format/-l` can be repeated as option-value pairs e.g. `ps --uuid <ID1> --uuid <ID2>`  and can be mixed e.g. `ps --uuid <ID> -n <name>`. Regex is supported.

An example of the output after running `boot` and `ps` in `process-manager-shell`
```bash
drunc-process-manager > ps
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

### `kill`
Kills processes and frees their memory. `kill`ed processes will not appear with `ps`.

There are no mandatory arguments. This command will by default not kill anything.

Options are:
 - `--session/-s`: kill processes from a specific session.
 - `--name/-n`: kill processes with a specific name.
 - `--user/-u`: kill processes from a specific user.
 - `--uuid`: kill processes with a specific unique identifier.

All options can be repeated as option-value pairs e.g. `kill --uuid <ID1> --uuid <ID2>` and can be mixed e.g. `kill --uuid <ID> -n <name>`. Regex is supported.

Example output after running `boot` and `ps` in `process-manager-shell`:
```bash
drunc-process-manager > kill -n mlt
                                                  Killed process
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━┓
┃ session      ┃ friendly name ┃ user     ┃ host      ┃ uuid                                 ┃ alive ┃ exit-code ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━┩
│ test-session │     mlt       │ pplesnia │ localhost │ e1f45c8c-1505-46b9-bb4f-a05b138ea314 │ False │ 255       │
└──────────────┴───────────────┴──────────┴───────────┴──────────────────────────────────────┴───────┴───────────┘
```
This shows that the `mlt` was killed, you can run `ps` and see that the process isn't here anymore.


Example output after running `boot` and `ps` in `process_manager`
```bash
[14:13:44] INFO     "ssh-process-manager": Killing                                                                                                                    ssh_process_manager.py:416
           WARNING  "ssh-process-manager": Killing all the known processes before exiting                                                                             ssh_process_manager.py:418
           INFO     "process_manager": Sending signal 'Signals.SIGQUIT' to 'e1f45c8c-1505-46b9-bb4f-a05b138ea314'                                                      ssh_process_manager.py:95
           INFO     "ssh-process-manager": Process 'mlt' (session: 'test-session', user: 'pplesnia') process exited with exit code 255                                ssh_process_manager.py:198
```

### `flush`
Removes dead processes from `process_manager` memory, effectively rendering them not restartable and not appearing in `ps`.

There are no mandatory arguments. This command will by default flush all the dead processes.

Options are:
 - `--session/-s`: flush processes from a specific session.
 - `--name/-n`: flush processes with a specific name.
 - `--user/-u`: flush processes from a specific user.
 - `--uuid`: flush processes with a specific unique identifier.

All options can be repeated as option-value pairs e.g. `flush --uuid <ID1> --uuid <ID2>` and can be mixed e.g. `flush --uuid <ID> -n <name>`. Regex is supported.

Example output after running `boot`, `ps`, `kill`, and killing `hsi-to-tc-app` (in a separate shell) in `process-manager-shell`
```bash
drunc-process-manager > ps
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
│ test-session │     hsi-to-tc-app         │ pplesnia │ localhost │ ec82eee5-fed9-4906-992c-8721d3be6f7f │ False │ 143       │
│ test-session │   hsi-controller          │ pplesnia │ localhost │ f1765e99-80bf-4a39-91f5-a56bc36962db │ True  │ 0         │
│ test-session │     hsi-01                │ pplesnia │ localhost │ cef1b4ff-6fff-4efc-a66a-08a9dce28c24 │ True  │ 0         │
└──────────────┴───────────────────────────┴──────────┴───────────┴──────────────────────────────────────┴───────┴───────────┘
drunc-process-manager > flush
                                                   Flushed process
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━┓
┃ session      ┃ friendly name     ┃ user     ┃ host      ┃ uuid                                 ┃ alive ┃ exit-code ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━┩
│ test-session │     hsi-to-tc-app │ pplesnia │ localhost │ ec82eee5-fed9-4906-992c-8721d3be6f7f │ False │ 143       │
└──────────────┴───────────────────┴──────────┴───────────┴──────────────────────────────────────┴───────┴───────────┘
drunc-process-manager > ps
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
│ test-session │   hsi-controller          │ pplesnia │ localhost │ f1765e99-80bf-4a39-91f5-a56bc36962db │ True  │ 0         │
│ test-session │     hsi-01                │ pplesnia │ localhost │ cef1b4ff-6fff-4efc-a66a-08a9dce28c24 │ True  │ 0         │
└──────────────┴───────────────────────────┴──────────┴───────────┴──────────────────────────────────────┴───────┴───────────┘
```


### `restart`
Restarts a `process_manager` process. If the process is alive, it `kill`s it and `boot`s it again.

There are no mandatory arguments. `process_manager` will ensure that you are restarting one and only one process. The `CLI` will check that you have provided at least one option below.

Options are:
 - `--session/-s`: restart processes from a specific session.
 - `--name/-n`: restart processes with a specific name.
 - `--user/-u`: restart processes from a specific user.
 - `--uuid`: restart processes with a specific unique identifier.

Options cannot be repeated. Applications are `restart`ed individually.

`test-session` output after running `boot`, `ps`, `kill`, `ps`, and `restart` in `process-manager-shell`
```bash
drunc-process-manager > restart -n df-02
```
`test-session` output after running `boot`, `ps`, `kill`, `ps`, and `restart` in `process_manager`
```bash
           INFO     "ssh-process-manager": Process 'df-02' (session: 'test-session', user: 'pplesnia') process exited with exit code 255                              ssh_process_manager.py:198
[14:17:14] INFO     "ssh-process-manager": Booted df-02 uid: eefc8fce-2b4d-4de1-9d20-1c209902f98e                                                                     ssh_process_manager.py:299
```

### `logs`
Prints logs from individual processes.

The `process_manager` will ensure that you are trying to fetch the logs from exactly one process. At least one of these arguments is required
 - `--session/-s`: fetch logs from the processes in the provided session.
 - `--name/-n`: fetch logs from the process with the provided name.
 - `--user/-u`: fetch logs from the processes spawned by the provided user.
 - `--uuid`: fetch logs from the process with the provided unique identifier.

The options can be repeated, but can only refer to a single process so typically options are not repeated here. Note - for `--session/-s` and `--user/-u` this choice will typically select multiple processes which `logs` will not process, and so are rarely used.

You can also provide the following options
 - `--how-far`: how many lines to fetch.
 - `--grep`: `grep` for a specific pattern in the logs.

Example output after running `boot`, `ps`, `kill`, `ps`, `restart`, `ps` and `logs` in `process-manager-shell`
```bash
drunc-process-manager > logs -n root-controller --how-far 5
────────────────────────────────────────────────────────────────────────── a61ffe46-dfa2-4a90-b888-7901fa5755b2 logs ───────────────────────────────────────────────────────────────────────────
           INFO     "Controller": 'df-controller@localhost:5600' (type ChildNodeType.gRPC)                                           controller.py:123
           INFO     "Controller": 'trg-controller@localhost:5700' (type ChildNodeType.gRPC)                                          controller.py:123
           INFO     "Controller": 'hsi-controller@localhost:5800' (type ChildNodeType.gRPC)                                          controller.py:123
           INFO     "Broadcast": ready                                                                                          broadcast_sender.py:65
root-controller was started on localhost:3333
───────────────────────────────────────────────────────────────────────────────────────────── End ──────────────────────────────────────────────────────────────────────────────────────────────
```
There is no log in the `process_manager`.

### terminate
Kills all running processes controlled by the process manager. There are no arguments required.

`test-session` output after running all the previous commands and `terminate` in `process-manager-shell`
```bash
drunc-process-manager > terminate
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
drunc-process-manager > ps
                         Processes running
┏━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━┳━━━━━━┳━━━━━━┳━━━━━━━┳━━━━━━━━━━━┓
┃ session ┃ friendly name ┃ user ┃ host ┃ uuid ┃ alive ┃ exit-code ┃
┡━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━╇━━━━━━╇━━━━━━╇━━━━━━━╇━━━━━━━━━━━┩
└─────────┴───────────────┴──────┴──────┴──────┴───────┴───────────┘
```
