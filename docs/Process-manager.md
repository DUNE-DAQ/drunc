# `drunc-process-manager`
This app is responsible for the `drunc` processes management. If you are running with `drunc-unified-shell`, both the `proces_manager` and `process-manager-shell` outputs will be dumped to the `drunc-unified-shell` `stdout`.

For a standalone `process_manager` you will need two shells - one shell to run the `process_manager`, and the other will interact with the `process_manager` through a `process-manager-shell`

## Configurations
To boot a `process_manager`, you will need to choose the most appropriate configuration that applies to the use case. The configurations that are packaged with `drunc` are defined in `drunc/src/data/process_manager/`, which are
* `ssh-standalone.json`: `ssh` based standalone implementation without a `kafka` feed.
* `ssh-pocket-kafka.json`: `ssh` based implementation with `pocket`'s `kafka` for message broadcasting.
* `ssh-CERN-kafka.json`: `ssh` based implementation with `kafka` service running at ENH1.
* `ssh-CERN-kafka-OpMon.json`: `ssh` based implementation with `kafka` service running at ENH1, and with Opmon.
* `k8s.json`: `kubernetes` implementation (not recommended nor working, so don't use this unless you are an working on getting it to work).

## Starting
Note that this runs the process manager service, _you will not be able to do anything else with it other than starting it and ctrl-c it_.

This goes as
```bash
drunc-process-manager configuration_file
```

### Example
To start the ssh version without kafka:
```bash
drunc-process-manager ssh-standalone
Using 'file://src/drunc/data/process_manager/ssh-standalone.json' as the ProcessManager configuration
Starting 'SSHProcessManager'
[12:43:26] INFO     "BroadcastSenderConfHandler": None                                                                                                                                                  configuration.py:25
           INFO     "Controller": DummyAuthoriser ready                                                                                                                                              dummy_authoriser.py:13
ProcessManager was started on np04-srv-019:10054
```
Once this is done, you will not be able to send commands to the process from the current shell with the `process_manager` acting in the foreground. To interact with a standalone instance of `process_manager` you will need to connect to it (see below).

# `drunc-process-manager-shell`

## Starting
This is done directly with the address of the process manager in a separate shell. When spawning a standalone instance, the port to connect through is printed in the last line. Communication is done using `gRPC`. The connection command is
```bash
drunc-process-manager-shell grpc://<hostname>:<port>
```

### Example
For the example spawned above, in the `process-manager-shell` this is
```bash
drunc-process-manager-shell grpc://localhost:10054
```

## Commands
This interface is very similar to the `drunc-unified-shell`. Each `drunc-process-manager-shell` command is listed below.

### `boot`
#### Description
This command spawns the processes that are used by the DAQ. In most cases, it SSHes on the host where the process is supposed to run, and execute the `daq_application` or `drunc-controller` binary.

The `boot` command will check if there are processes running in the process manager with the same session name and ask for confirmation if it detects other process running under the same session name.

Command arguments (in this order):
* `configuration_file`: path to the system configuration file (xml `OKS` file).
* `configuration_session_id`: name of the session defined in the system configuration file.
* `session_name`: name of the session.

Command options:
* `--override-logs/--no-override-logs` (optional), this flags adds a timestamp to the log files of the application, effectively making them non-overriding. Note this happens only in the case where the `log_path` is _not_ set in your configuration's `Session` or `Application` objects. If the configuration's `log_path` is not `./` in either of these, the run control will use that, and the log will not be overriding (in this case, _this flag is ignored_).
* `-u/--user` (optional), assigns an owner to the spawned processes, default is `$USER`.

Caveats:
* It is most likely impossible to specify a `user` different from the one that is running the `process_manager`, simply because that user will likely not have the ssh keys necessary to ssh on a different host as a different user.

#### Example
```bash
boot config/daqsystemtest/example-configs.data.xml local-1x1-config plasorak-test
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

### `ps`
See `ps`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#ps)

### `kill`
See `kill`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#kill)

### `flush`
See `flush`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#flush)

### `restart`
See `restart`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#restart)


### `logs`
See `logs`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#logs)

### terminate
See `terminate`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#terminate)

