# `drunc-unified-shell`
This is a merger of the `drunc-process-manager-shell` and `drunc-controller-shell` (for the root controller). Both of them are described [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-manager) and [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Controller), respectively. As we will see, there are just a couple of differences.

## Starting the shell
Is done by specifying the process manager configuration (see [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-manager#Configuration)), DAQ configuration, and session name, as such
```bash
drunc-unified-shell <process_manager_config> <daq_config> <session_name>
```
To continue with our timeless example, this is what it looks like with the `daqsystemtest` example `local-1x1-config` session defined [in this file](https://github.com/DUNE-DAQ/daqsystemtest/blob/develop/config/daqsystemtest/example-configs.data.xml), if you are running without Kafka:
```bash
drunc-unified-shell ssh-standalone config/daqsystemtest/example-configs.data.xml local-1x1-config
```

You then have access to all the functionality in the [`process_manager`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-manager) and the [`controller`](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Controller).

## Commands
### `boot`
The _only difference_ is the `boot` command which now does not arguments, but is still able to take the optional arguments:
 - `--no-override-logs/--override-logs`: decides whether to override the logs, if `--override-logs` is used the log filenames will not include a timestamp, otherwise if `--no-overrride-logs` is the log filenames will include a timestamp. By default, the logs are overwritten.
 - `-l/--log-level`: sets the log level.
 - `-u/--user`: assigns an owner to the spawned processes, default is `$USER`.
