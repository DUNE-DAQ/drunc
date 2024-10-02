# `drunc-controller`
This app is responsible for propagating commands to its children, and ensuring that they are correctly defined. It is spawned directly when you use `process_manager`'s `boot`. **You should not attempt to execute `drunc-controller`, unless you know what you are doing.** The `root-controller` is in charge of communicating with all of the segment controllers, and the segment `controller`s (subcontrollers) are in charge of communicating with all the segment applications. You can interface with the `root-controller` directly through either `drunc-unified-shell` or `drunc-controller-shell`, and you can interact with the subcontrollers through `drunc-controller-shell`. The port through which the communication is sent is defined in the connectivity service, or can be accessed through the `controller`'s logs.

# `drunc-controller-shell`
This is the interface through which the user interacts with the `root_controller`. This output is the same for the `controller-shell` as it is for the `unified-shell`. Each available `controller-shell` command will be described here.

## `connect`
Currently broken, return to me.

## `exclude` (`include`)
`exclude`s (`include`s) the current `controller` and all of its children from executing `fsm` commands.

## `conf`, `start`, `enable-triggers`, etc
These commands are generated automatically from the controller, which in turns comes from its configuration. If a transition has an argument or an option needed, you can access it via the command help:
```bash
conf
start --help
Usage: drunc-unified-shell PROCESS_MANAGER_CONFIGURATION BOOT_CONFIGURATION SESSION_NAME start
           [OPTIONS] RUN_NUMBER

  Execute the transition start on the controller root-controller

Options:
  --file-logbook-post TEXT
  --trigger-rate FLOAT
  --disable-data-storage BOOLEAN
  --help                          Show this message and exit.
start 123456
enable-triggers
[etc...]
```
More details on the available FSM commands is provided [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/FSM).

## `quit`
In `drunc-unified-shell`, this closes the managed applications and the `unified-shell`, returning back to the bash shell. In `drunc-controller-shell`, this closes the connection to the `controller`.

## `surrender-control` (`take-control`)
Surrenders (takes) control of the applications and controllers managed by the current controller. If these are executed from the `root-controller` (default controller from which commands are sent with `drunc-unified-shell`), this applies to all applications. If these are executed from one of the subcontrollers, it will apply to that segment only.

## `take-control`
Takes control of the applications and controllers managed by the current controller. See `surrender-control`.

## `who-is-in-charge`
Prints the person who is in charge of the current run. Only the person in charge can issue write operations (for example issue a transition) to the controller.

## `describe`
Describes the controller endpoint commands, which define all the `gRPC` calls. The printed command descriptions are not designed to be available to the user through the shell, they are for internal use. [Sample output](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Sample-outputs#describe).

## `exit`
Works the same way as `quit`.

## `help`
Gives you help with commands that have been documented. This can be used as just `help` to declare all the commands that are available at any given time, or as `help <command>` describes what `<command>` does and its arguments. [Sample output](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/#help---from-drunc-unified-shell).

## `ls`
List all the processes under the control of the current controller. For `drunc-unified-shell` this lists the segment controllers, and for `drunc-controller-shell` this includes all the applications. [Sample output](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/#ls---from-drunc-unified-shell).

## `status`
Defines the status of the directly controlled applications. [Sample output](hhttps://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/#status---from-drunc-unified-shell).

## `whoami`
Prints `${USERNAME}` (i.e. the person logged in).
