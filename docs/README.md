# DUNE Run Control (drunc)

This software defines a flexible run control infrastructure for a distributed DAQ system defined in a set of configuration files used at run time for DUNE. Operation of the experiment is defined through a finite state machine (FSM) which describes the operational state of the DAQ.

The project is still under development, and as such will still have bugs or features that users may not want. If you encounter any of these please raise an issue and describe it clearly so that we can resolve it easily.

![drunc_overview](https://github.com/DUNE-DAQ/drunc/blob/develop/docs/drunc_overview.png)

# Setting up
If you are trying to run `drunc` you **must** have a DUNE-DAQ environment, with `cvmfs` available. See setup instructions [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/daq-buildtools/) to setup an nightly or a static release.

# Running drunc
Once you have drunc, you can look at the [quick start instructions](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Running-drunc).

If you need more details:
 - [Process manager](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-manager)
 - [Controller](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Controller)
 - [Unified shell](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Unified-shell) (merges the controller and process manager shells)

There are more in depth descriptions of part of the system here:
 - [FSM](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/FSM)

Finally, we have a [FAQ](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/FAQ), have a look there if you have a problem!

# Developing
If are developing a user interface for `drunc`, you can get help here:
 - [Messaging format](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Messaging-format) (valid for all the `drunc` endpoints)
 - [Process manager endpoint description](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Process-Manager-interface)
 - [Controller endpoint description](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Controller-interface)

There is more developer information in the [drunc wiki](https://github.com/DUNE-DAQ/drunc/wiki).

# Release notes
... are [here](https://dune-daq-sw.readthedocs.io/en/latest/packages/drunc/Release-notes)