# v0.10.1
Adds a hierarchy to the processes when they're spawned, so the trees with `ps` get correctly constructed

# v0.10.0
Combining all of v0.8.0, v0.9.0, and v0.10.0
## Removing zombie processes
Gunicorn interprets `SIGTERM` as restart, an `EXIT` trap to kill all the processes actually introduced restarts to connectivity applications and the controllers were not being killed. The trap is now removed and `SIGTERM` is not sent on kill.

## Packaging `drunc-unified-shell` configurations with the code
You can now spawn the unified shell with 
```bash
drunc-unfified-shell <conf>
```
for which `<conf>` is one of the configuration files found in `drunc/src/drunc/data/process_manager`, or the usual file directories. The files no longer need a `file://` schema declaration if the file is found in `$PWD`or `data/process_manager`.

## Usability fixes
Operational fixed were defined, mainly
### Client side FSM sequences
A sequence of fsm transitions are grouped into sequences, as defined [here](https://github.com/DUNE-DAQ/drunc/wiki/FSM). 
Once a transition is completed, `drunc-unified-shell` will contain output that defines what state the system is in and what transitions can be done.
### Disabled segments 
The disabled segments are no longer attempted to be booted.
### ELisA microservice
When transitions are executed, log messages can be sent to ELisA directly from the CLI. More details are defined [here](https://github.com/DUNE-DAQ/drunc/pull/113)

## Cosmetic fixes
### `ps` now contains the process host
Useful for devleoping NP02 configuration files.

# v0.7.0

## The Big JSON Drop
We're now only supporting OKS configuration, except for the Process manager, which uses a simple JSON configuration.

## Run number
The user now has to specify the run number when starting a run 
`drunc-unified-shell > fsm start run_number 1234`

Remember, there is always the describe command to understand the arguments you need to provide to FSM commands:
```
drunc-unified-shell > describe --command fsm
                                             root-controller.test-session (controller) commands
┏━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ name  ┃ input type                ┃ return type                       ┃ help ┃ Command arguments                                          ┃
┡━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ start │ controller_pb2.FSMCommand │ controller_pb2.FSMCommandResponse │      │ run_number (INT MANDATORY) default: <no_default> help:     │
│       │                           │                                   │      │ disable_data_storage (BOOL OPTIONAL) default: False help:  │
│       │                           │                                   │      │ trigger_rate (FLOAT OPTIONAL) default: 1.0 help:           │
│       │                           │                                   │      │ message (STRING OPTIONAL) default:  help:                  │
│ scrap │ controller_pb2.FSMCommand │ controller_pb2.FSMCommandResponse │      │                                                            │
└───────┴───────────────────────────┴───────────────────────────────────┴──────┴────────────────────────────────────────────────────────────┘
```
## FSM execution report
This now looks a bit more instructive:
```
                    conf execution report
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
┃ Name                 ┃ Command execution ┃ FSM transition ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
│ root-controller      │ success           │ success        │
│   hsi-controller     │ success           │ success        │
│     hsi-01           │ success           │ success        │
│   ru-controller      │ success           │ success        │
│     ru-01            │ success           │ success        │
│   df-controller      │ success           │ success        │
│     dfo-01           │ success           │ success        │
│     tp-stream-writer │ success           │ success        │
│     df-01            │ success           │ success        │
│     df-02            │ success           │ success        │
│   trg-controller     │ success           │ failed         │
│     mlt              │ failed            │ failed         │
│     tc-maker-1       │ failed            │ failed         │
│     hsi-to-tc-app    │ failed            │ failed         │
└──────────────────────┴───────────────────┴────────────────┘
```

## `FSMInterface` -> `FSMAction`
A big renaming of the FSM interfaces to something better.

## Exceptions
Most of the server side exceptions should now be propagated back to the shell

## Thread pinning
If you are using `fsm.data.xml` from `appdal`, [here](https://github.com/DUNE-DAQ/appdal/blob/develop/config/appdal/fsm.data.xml), you will now get an example of thread pinning. These happen:
 - just before `conf`
 - just after `conf`
 - just after `start`

Right now the file is the same for all the three transition, but hopefully it's not too complicated for you to understand what you'd need to modify to get different ones.

## File run registry
The configuration you are running from is `cp`ed to a file in `PWD` called `run_conf"+str(run_number)+".data.xml`

## File log book
A file called `logbook.txt` is created (or appended) at the start of the run, with messages such as: `user is starting run 123`

## Process manager kill command
This command now also flushes dead processes, so no need `kill` and `flush` when you want to exit.
Beside this, the kill command now sends, sequentially `SIGINT`, `SIGKILL` and `SIGQUIT`, to the ssh process that needs to be killed. There is a configurable timeout in between the sequence.

## Unified shell starts the process manager
So no need to open the Process manager in a different shell.

## Initial K8s process manager
There is something, but this isn't quite finished yet, so hold off using it.

## Empty run time environment setup scripts
Can now be provided in the configuration, and the shell will fill it for you.

## Bug fixes

### SSH Process manager restart
Didn't really work, now should work.