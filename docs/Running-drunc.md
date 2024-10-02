# Running `drunc`
This can currently be done either with `drunc-unified-shell`, which is recommended for first time/casual users, or with a `process-manager-shell` and `controller-shell`, which is recommended for expert developers.

All of the `drunc` shells support tab completion.

## With `drunc-unified-shell`
### Spawning the shell and `process_manager`
In your terminal window run `drunc-unified-shell` as
```bash
drunc-unified-shell <process_manager_configuration> <DAQ_configuration_file> <session_name>
```
For which `<process_manager_configuration>` can be
 - The name of a configuration file defined in `drunc/src/drunc/data/process_manager/` (see them [here](https://github.com/DUNE-DAQ/drunc/tree/develop/src/drunc/data/process_manager)). There is a description of them in [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-manager#Configurations)
 - The path to name of a custom configuration file, relative or absolute
 - `<DAQ_configuration_file>` - the name of the DAQ system configuration file, typically defined in [`daqsystemtest/config/example-configs.data.xml`](https://github.com/DUNE-DAQ/daqsystemtest/blob/develop/config/daqsystemtest/example-configs.data.xml)
 - `<session_name>` - the name of the session defined in `<DAQ_configuration_file>`


### Interacting with `process_manager`
At this point the `process_manager` has been spawned, and you can interface it directly through the current shell. You can now start the DAQ processes as
```bash
drunc-unified-shell > boot
```
You will get a lot of output, it will finish with something like:
```bash
[11:54:36] INFO     "unified": localhost:3333 is 'root-controller.test-session' (name.session), starting listening...                                                                shell_utils.py:284
Connected to the controller
root-controller.test-session's children 👪: ['ru-controller', 'df-controller', 'trg-controller', 'hsi-controller']
[...]
           INFO     "unified": You are in control.
```
Notice the last message indicates that you have taken control of the root controller and all its children (so connections work).

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

We have started a couple of processes - the standard DAQ applications organized into segments each with a controller, and a `root-controller` that will control all the segments. The logs and work directory is the `${PWD}` where you executed the `drunc-unified-shell`. From here you can use the `process_manger` commands
 - `ps` - list all the processes
 - `kill` - kill specific processes
 - `flush` - remove dead processes
 - `restart` - restart processes
 - `logs` - show the logs of specific processes
 - `terminate` - kill all the processes

There are many more functionalities to the shell, head over to the [process manager documentation](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-manager) to see how to interact with it.

### Interacting with `root-controller`
Next, let's send commands to the `root-controller`. These commands will be propagated by it to other applications using `gRPC`. To see which segments a controller controls, you can use `ls`:
```bash
drunc-unified-shell > ls
['ru-controller', 'df-controller', 'trg-controller', 'hsi-controller']
```
The set of commands that you can send to the `root-controller` are
 - `describe` - shows available _endpoint_ commands (not this currently describes what the controller endpoint can do internally not through the shell)
 - `ls` - lists all running segments
 - `status` - lists the FSM state, substate, error status, and included parameters of the segments
 - `connect` - remaps the root controller
 - `take_control` - updates the user in charge (actor)
 - `surrender_control` - releases the current actor
 - `who_am_i` - prints your username
 - `who_is_in_charge` - prints who is in charge of the root controller
 - `include` - includes self in the current session
 - `exclude` - excludes self from the current session

[FSM](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/FSM) transitions can be executed directly from the shell with the commands:
 - `conf` - configure the applications by ingesting the parameters from the configuration file to the applications
 - `start` - start a run, allocating a run number. Initializes queues and connections
 - `enable-triggers` - start generating TPs, TDs are not propagated to the DFO
 - `disable-triggers` - stop collecting generated TPs to file
 - `drain-dataflow` - stop propagating TDs to the TRBs.
 - `stop-trigger-sources` - stop generating TPs
 - `stop` - stop app communication
 - `scrap` - remove all the configuration parameters from the applications

## Operating a DAQ with `drunc`
Let's take the DAQ for a spin:
```bash
drunc-unified-shell > conf
drunc-unified-shell > start 12345 # start the run 12345
drunc-unified-shell > enable-triggers
[...we wait for a bit of time, to get a file...]
drunc-unified-shell > disable-triggers
drunc-unified-shell > drain-dataflow
drunc-unified-shell > stop-trigger-sources
drunc-unified-shell > stop
drunc-unified-shell > scrap
```

Several things to note:
 - There is a profusion of logging that happens. This is coming from the asynchronous logging from the controller. If someone tries to connect at the same time or to execute a command you will see it appearing on your shell too (you can try it yourself in a different shell). Unfortunately CLIs do not lend itself very well with this, and one needs a better UI for the logs to be rendered better and to not distract.
 - The `describe` command describes the _endpoint_ **NOT** the shell. This means that there are some commands that are not directly available in the shell (for example `get_children_status`). However if you do `status` is the shell, you will get the children statuses because the shell calls `get_children_status` under the hood.

You can now head to the [controller documentation](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Controller) for more information.

# Shutting down
```bash
drunc-unified-shell > kill --session test-session
drunc-unified-shell > quit
```
or
```bash
drunc-unified-shell > terminate
drunc-unified-shell > quit
```