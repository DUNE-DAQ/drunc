# drunc FAQ

## `ServerUnreachable` / `failed to connect to all address`
Note: This has been patched since `v0.11.0`, it is hence recommended that you update the version that you are using if possible.

The connectivity service has statically defined ports, hence you need to check if there are any other `drunc` users on the physical host you are running on. If there are, when you `boot` you will likely get an error of
```
drunc.utils.grpc_utils.ServerUnreachable: ('failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:3333: connection attempt timed out before receiving SETTINGS frame', 14)
```
To resolve this issue, the current recommendation is to use a different physical host on which there are no other `drunc` users.

## I am receiving some strange `ssh` errors...
Chances are that you cannot actually ssh onto the named servers. It is recommended that you check whether you can `ssh` onto the servers required by your configuration using `drunc-ssh-doctor`:
```bash
drunc-ssh-doctor check-session name/of/your/file.xml session-name
```

Alternatively, you can check each host individually:
```bash
drunc-ssh-doctor check-host localhost # or np04-srv-019 etc.
```

This will tell you which server you cannot `ssh` to and how.

## I can't ssh on that host!
So, you've just run `drunc-ssh-doctor` and it responded back with
```text
name-of-your-host [default]: ❌
name-of-your-host [publickey]: ❌
name-of-your-host [gssapi-with-mic]: ❌
```

### SSH keys (preferred solution)

Simplest is to use SSH keys, here is how to do it:
```bash
ssh-keygen
```
then press `<Enter>` tree times when prompted where to put the key and for a password (*do not* enter a password here, and the default location for the key is also fine).

Then do:
```bash
ssh-copy-id localhost # You're using network mounted storage right?
```

This command should prompt you a password, for the last time. This is the same password you used to log on the server. After that you can do:
```bash
ssh name-of-your-host
```
and you won't be prompted for a password ever again!

Then in `~/.ssh/config` (which you should create if it doesn't exist) add:
```config
Host name-of-the-host
    User your-user-name
    PasswordAuthentication no
```

Run the `drunc-ssh-doctor` once more, to make sure the SSH-Key auth works. You should get:
```
name-of-the-host [publickey]: ✅
```

### Kerberos
Only use this if the approach above with SSH keys didn't work. Drunc does not multiplex SSH connections, so if you start 20 applications, the kerberos server gets hit 20 times with authorisation request, more or less at the same time. This makes this a bit less reliable that standard SSH keys.

To get this to work, create or edit  `~/.ssh/config` and add:
```config
Host name-of-the-host
    GSSAPIAuthentication yes
    GSSAPIDelegateCredentials yes
```

Then run
```bash
kinit your-username@CERN.CH
# or, if you're at FNAL
# kinit your-username@FNAL.GOV
```
Enter your password, run the `drunc-ssh-doctor` once more and make sure you get
```text
name-of-your-host [gssapi-with-mic]: ✅
```

Note that you will need to enter `kinit` every once in a while (between one day and one week).

## What SSH commands are actually run?
The simplest to know how the processes are started is to add the option `--log-level debug` for the process manager shell or the unified shell.

## Do you have unit tests?
Sure,
```bash
cd drunc/
pytest
```
All of the tests are in `tests` and follow the same hierarchy as the code (so for example, the unit tests of the module `drunc.utils.utils` is in `tests/utils/test_utils.py`).

## An application has crashed, how do I stop the DAQ?
Let's say the application that has crashed is the `mlt`, which belongs to the `trg-segment`, it is controlled by the `trg-controller`. Status display something like the following:
```
                                           local-1x1-config status
┏━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Name                   ┃ Info      ┃ State   ┃ Substate ┃ In error ┃ Included ┃ Endpoint                  ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ root-controller        │           │ running │ running  │ No       │ Yes      │ grpc://10.73.136.38:46381 │
│   ru-controller        │           │ running │ running  │ No       │ Yes      │ grpc://10.73.136.38:37377 │
│     ru-01              │ conn apa1 │ running │ idle     │ No       │ Yes      │ rest://10.73.136.38:50003 │
│   hsi-fake-controller  │           │ running │ running  │ No       │ Yes      │ grpc://10.73.136.38:46063 │
│     hsi-fake-01        │           │ running │ idle     │ No       │ Yes      │ rest://10.73.136.38:43519 │
│     hsi-fake-to-tc-app │           │ running │ idle     │ No       │ Yes      │ rest://10.73.136.38:57553 │
│   trg-controller       │           │ running │ running  │ No       │ Yes      │ grpc://10.73.136.38:45141 │
│     tc-maker-1         │           │ running │ idle     │ No       │ Yes      │ rest://10.73.136.38:35081 │
│     mlt                │           │ running │ idle     │ Yes      │ Yes      │ rest://10.73.136.38:39393 │
│   df-controller        │           │ running │ running  │ No       │ Yes      │ grpc://10.73.136.38:36513 │
│     tp-stream-writer   │           │ running │ idle     │ No       │ Yes      │ rest://10.73.136.38:37369 │
│     dfo-01             │           │ running │ idle     │ No       │ Yes      │ rest://10.73.136.38:55299 │
│     df-01              │           │ running │ idle     │ No       │ Yes      │ rest://10.73.136.38:54177 │
└────────────────────────┴───────────┴─────────┴──────────┴──────────┴──────────┴───────────────────────────┘
```
In this case, you can do:
```
connect grpc://10.73.136.38:45141 # connect to the trg-controller
exclude # exclude the trg-controller and all of its children
connect grpc://10.73.136.38:46381 # connect to the root-controller
drain-dataflow
stop-trigger-sources
...
```

Note, if it's a controller that has crashed, there is no way to exclude that child from the `root-controller` (so you can `exit` to kill everything).

## One of my application crashed/failed during a transition
The best way to get out of this is to exclude the application/segment that got into error.
Let's say you find yourself in this situation:
```
drunc-unified-shell > status
                                                local-1x1-config status
┏━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Name                   ┃ Info      ┃ State      ┃ Substate      ┃ In error ┃ Included ┃ Endpoint                    ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ root-controller        │           │ configured │ configured    │ No       │ Yes      │ grpc://131.225.193.20:45817 │
│   hsi-fake-controller  │           │ configured │ configured    │ Yes      │ Yes      │ grpc://131.225.193.20:46009 │
│     hsi-fake-to-tc-app │           │ initial    │ executing_cmd │ Yes      │ Yes      │ rest://131.225.193.20:49359 │
│     hsi-fake-01        │           │ configured │ idle          │ No       │ Yes      │ rest://131.225.193.20:42045 │
│   ru-controller        │           │ configured │ configured    │ No       │ Yes      │ grpc://131.225.193.20:45185 │
│     ru-01              │ conn apa1 │ configured │ idle          │ No       │ Yes      │ rest://131.225.193.20:55821 │
│   trg-controller       │           │ configured │ configured    │ No       │ Yes      │ grpc://131.225.193.20:38205 │
│     mlt                │           │ configured │ idle          │ No       │ Yes      │ rest://131.225.193.20:58095 │
│     tc-maker-1         │           │ configured │ idle          │ No       │ Yes      │ rest://131.225.193.20:33781 │
│   df-controller        │           │ configured │ configured    │ No       │ Yes      │ grpc://131.225.193.20:38587 │
│     dfo-01             │           │ configured │ idle          │ No       │ Yes      │ rest://131.225.193.20:51289 │
│     tp-stream-writer   │           │ configured │ idle          │ No       │ Yes      │ rest://131.225.193.20:40975 │
│     df-01              │           │ configured │ idle          │ No       │ Yes      │ rest://131.225.193.20:36373 │
└────────────────────────┴───────────┴────────────┴───────────────┴──────────┴──────────┴─────────────────────────────┘
```
You can always connect to a different controller using the `connect` command:
```
drunc-unified-shell > connect grpc://131.225.193.20:46009
[2025/03/03 10:31:03] INFO       commands.py:68                 drunc.controller.interface:                   Already connected to a controller (root-controller.local-1x1-config@131.225.193.20:45817)
Do you want to disconnect from it before? [y/N]: y
[2025/03/03 10:31:03] INFO       commands.py:71                 drunc.controller.interface:                   Disconnecting...
[2025/03/03 10:31:03] INFO       shell_utils.py:283             drunc.utils.ShellContext:                     You will not be able to issue command to root-controller.local-1x1-config anymore.
[2025/03/03 10:31:03] INFO       shell_utils.py:285             drunc.utils.ShellContext:                     Driver 'controller' has been deleted.
[2025/03/03 10:31:03] INFO       commands.py:74                 drunc.controller.interface:                   Connecting this shell to the controller at grpc://131.225.193.20:46009...
⠋ Trying to talk to the controller... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ -:--:-- 0:00:00
drunc-unified-shell >
```
Then, you can `exclude` that application:
```
drunc-unified-shell > exclude hsi-fake-to-tc-app
[2025/03/03 10:31:27] INFO       commands.py:160                drunc.controller.interface:                   children excluded: hsi-fake-to-tc-app
```
Make the controller come out of error:
```
drunc-unified-shell > recompute-status
                                            local-1x1-config status
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Name                 ┃ Info ┃ State      ┃ Substate      ┃ In error ┃ Included ┃ Endpoint                    ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ hsi-fake-controller  │      │ configured │ configured    │ No       │ Yes      │ grpc://131.225.193.20:46009 │
│   hsi-fake-to-tc-app │      │ initial    │ executing_cmd │ Yes      │ No       │ rest://131.225.193.20:49359 │
│   hsi-fake-01        │      │ configured │ idle          │ No       │ Yes      │ rest://131.225.193.20:42045 │
└──────────────────────┴──────┴────────────┴───────────────┴──────────┴──────────┴─────────────────────────────┘
[2025/03/03 10:31:35] INFO       shell_utils.py:303             drunc.utils.ShellContext:                     Current FSM status is configured. Available transitions are start, scrap.
```
Then, reconnect to the top controller:
```
connect grpc://131.225.193.20:45817
```
You can now issue `status`, or `recompute_status` and continue working. Of course, none of the command will be propagated to the application that was excluded (in this case `hsi-fake-to-tc-app`).


## So empty...
If you have a question, please reach out to developers or fill an issue [here](https://github.com/DUNE-DAQ/drunc/issues).

