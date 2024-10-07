# drunc FAQ

## ServerUnreachable / failed to connect to all address

The connectivity service has statically defined ports, hence you need to check if there are any other `drunc` users on the physical host you are running on. If there are, when you `boot` you will likely get an error of
```
drunc.utils.grpc_utils.ServerUnreachable: ('failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:3333: connection attempt timed out before receiving SETTINGS frame', 14)
```
To resolve this issue, the current recommendation is to use a different physical host on which there are no other `drunc` users.

## What SSH commands are actually run?
The simplest to know how the processes are started is to add the option `--log-level debug` for the process manager shell or the unified shell.

## So empty...
If you have a question, please reach out to developers or fill an issue [here](https://github.com/DUNE-DAQ/drunc/issues).