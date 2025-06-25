# Running `drunc`
This can currently be done either with `drunc-unified-shell`, which is recommended way to use the run control right now.

You can also run 3 terminals, one for the `drunc-process-manager`, another for the `drunc-process-manager-shell` and one more for the `drunc-controller-shell`, this is recommended for run control experts.

All of the `drunc` shells support tab completion.

## With `drunc-unified-shell`
### Reference
Can be found [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference).

### Spawning the shell and `process_manager`
In your terminal window, run `drunc-unified-shell`:
```bash
drunc-unified-shell process_manager_configuration config-file.data.xml configuration_session_id session_name
```
For which:
* `process_manager_configuration` is the name of a configuration file defined in `drunc/src/drunc/data/process_manager/` (see them [here](https://github.com/DUNE-DAQ/drunc/tree/develop/src/drunc/data/process_manager)). There is a description of them in [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-manager#Configurations). Alternatively, this can be the path to name of a custom configuration file, relative or absolute.
* `config-file.data.xml` is the name of the DAQ system configuration file, typically defined in [`daqsystemtest/config/example-configs.data.xml`](https://github.com/DUNE-DAQ/daqsystemtest/blob/develop/config/daqsystemtest/example-configs.data.xml).
* `configuration_session_id` is the name of the session defined in `config-file.data.xml`.
* `session_name` is a name you choose.

### Interacting with `process_manager`
At this point the `process_manager` has been spawned, and you can interface it directly through the current shell. You can now start the DAQ processes as
```bash
drunc-unified-shell > boot
```
You will get a lot of output, it will finish with something like:
```bash
[2025/04/08 17:50:38] INFO       commands.py:79                 unified_shell.boot:                           Booted successfully
```

You can then check the running processes with `ps` as
```bash
drunc-unified-shell > ps
                                                      Processes running
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━┓
┃ session      ┃ friendly name             ┃ user     ┃ host      ┃ uuid                                 ┃ alive ┃ exit-code ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━┩
│ test-session │ root-controller           │ pplesnia │ localhost │ cb986b26-221c-4819-bc9b-20825b11bee9 │ True  │ 0         │
│ test-session │   local-connection-server │ pplesnia │ localhost │ 0994f364-8d1f-4ccc-98f1-1872f8722b0f │ True  │ 0         │
│ test-session │   ru-controller           │ pplesnia │ localhost │ 2715c09e-d568-4e86-80a2-bb7827fd46fc │ True  │ 0         │
│ test-session │     ru-01                 │ pplesnia │ localhost │ d93f7922-377e-463f-8b26-523b73fb7d6b │ True  │ 0         │
│ test-session │   df-controller           │ pplesnia │ localhost │ d91c71f7-3c41-4a7e-aab0-7fb98d99c071 │ True  │ 0         │
| ...          | ...                       | ...      | ...       | ...                                  | ...   | ...       |
└──────────────┴───────────────────────────┴──────────┴───────────┴──────────────────────────────────────┴───────┴───────────┘
```

We have started a couple of processes - the standard DAQ applications organized into segments each with a controller, and a `root-controller` that will control all the segments. From here you can use the `process_manger` commands
* `ps`: list all the processes
* `kill`: kill specific processes
* `flush`: remove dead processes
* `restart`: restart processes (DO NOT USE)
* `logs`: show the logs of specific processes
* `terminate`: kill all the processes

### Interacting with `root-controller`
Next, let's send commands to the `root-controller`. These commands will be propagated by it to other applications using `gRPC`. To see which segments a controller controls, you can use `ls`:

The set of commands that you can send to the `root-controller` are
* `status`: lists the FSM state, substate, error status, and included parameters of the segments.
* `recompute-status`: calculates the status of the controllers from their children, and reset their status.
* `connect`: connects to another controller,
* `disconnect`: disconnects from a controller,
* `take-control`: updates the user in charge of the controller (DO NOT USE)
* `surrender-control`: releases the current user (DO NOT USE)
* `whoami`: prints your username (DO NOT USE)
* `who-is-in-charge`: prints who is in charge of the root controller (DO NOT USE)
* `include`: includes a children in the current session
* `exclude`: excludes a children from the current session
* `expert-command`: send abritrary json to an application

[FSM](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/FSM) transitions can be executed directly from the shell with the commands:
* `conf`: configure the applications by ingesting the parameters from the configuration file to the applications
* `start`: start a run, allocating a run number. Initializes queues and connections
* `enable-triggers`: start generating TPs, TDs are not propagated to the DFO
* `disable-triggers`: stop collecting generated TPs to file
* `drain-dataflow`: stop propagating TDs to the TRBs.
* `stop-trigger-sources`: stop generating TPs
* `stop`: stop app communication
* `scrap`: remove all the configuration parameters from the applications

### Typical operation of the DAQ
Let's take the DAQ for a spin.

First we start the unified shell:
```bash
drunc-unified-shell ssh-standalone config/daqsystemtest/example-configs.data.xml local-1x1-config ${USER}-test
```

We then boot the system:
```bash
boot
```

After that, we check the status:
```bash
status
```

If all looks good, you can:
```bash
conf
start --run-number 12345 # start the run 12345 in this case, but this command can differ depending on the configuration you are using
enable-triggers
# We wait for a "bit" of time, depending on what you are doing... Maybe until the end of your shift... or that an expert tells us to stop the run...
disable-triggers
drain-dataflow
stop-trigger-sources
stop
scrap
```

### Shutting down
```bash
kill --session test-session
quit
```
or
```bash
terminate
quit
```

### With the trio `drunc-process-manager`, `drunc-process-manager-shell`, `drunc-controller-shell`
The process manager documentation is [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-manager).

The controller shell documentation is [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Controller).
