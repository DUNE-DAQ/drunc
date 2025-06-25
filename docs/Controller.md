# `drunc-controller`
This app is responsible for propagating commands to its children, and ensuring that they are correctly defined. It is spawned directly when you use `process_manager`'s `boot`.

## Starting
**You should not attempt to execute `drunc-controller`, unless you know what you are doing.**

The `root-controller` is in charge of communicating with all of the segment controllers, and the segment `controller`s (subcontrollers) are in charge of communicating with all the segment applications. You can interface with the controllers directly through either `drunc-unified-shell` or `drunc-controller-shell`. The port through which the communication is sent is defined in the connectivity service, or can be accessed through the `controller`'s logs.

# `drunc-controller-shell`

## Starting
Is done by issuing:
```bash
drunc-controller-shell grpc://hostname:port
```

where you will have to replace the hostname and the port appropriately.

## Commands
This interface is very similar to the `drunc-unified-shell`. All the commands one can execute from the shell are listed below.

### `conf`, `start`, `enable-triggers`, `change-rate`, `disable-triggers`, `drain-dataflow`, `stop-trigger-sources`, `stop` and `scrap`
More details on the available FSM commands is provided [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/FSM).
* See `conf`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#conf)
* See `start`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#start)
* See `enable-triggers`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#enable-triggers)
* See `change-rate`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#change-rate)
* See `disable-triggers`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#disable-triggers)
* See `drain-dataflow`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#drain-dataflow)
* See `stop-trigger-sources`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#stop-trigger-sources)
* See `stop`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#stop)
* See `scrap`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#scrap)

### `connect`
See `connect`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#connect)

### `disconnect`
See `disconnect`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#disconnect)

### `exclude`
See `exclude`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#exclude)

### `expert-command`
See `expert-command`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#expert-command)

### `exit`
Works the same way as `quit`.

### `help`
Gives you help with commands that have been documented. This can be used as just `help` to declare all the commands that are available at any given time, or as `help <command>` describes what `<command>` does and its arguments.

### `include`
See `include`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#include)

### `quit`
This closes the connection to the `controller`.

### `recompute-status`
See `recompute-status`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#recompute-status)

### `status`
See `status`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#status)

### `surrender-control`
See `surrender-control`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#surrender-control)

### `take-control`
See `surrender-control`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#surrender-control)

### `who-is-in-charge`
See `surrender-control`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#surrender-control)

### `whoami`
See `whoami`'s documentation [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell-reference#whoami)
