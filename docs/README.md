# DUNE Run Control (drunc)

This is a skeleton of a run control to be used in DUNE. The project is still in its "fundation stage", which basically means it doesn't work for other than experimental purposes.

## Install

### Requirements
This python code uses `rich`, `gRPC`.

### How to install me on your machine
To start using this code, you need to:
```bash
python -m venv venv             # create a python venv
source setup.sh                 # enter the venv and setup the env variable DRUNC_DATA
pip install -r requirements.txt # ... install rich and gRPC
pip install .                # ... install this package if you are planning to modify you can use `pip install -e .`
```

Next time you loggin:
```bash
source setup.sh # enter the venv and setup the env variable DRUNC_DATA
```
_et voila._

### How to use the Docker image
You need to build the image:
```bash
docker build . -t drunc-image
```

Then:
```bash
you@your-machine $ docker run --rm -it --entrypoint bash drunc-image
root@07ea4b58b97d:/#
```

You can add a `-p 100:100` to the last `docker run` command to expose the port 100 to your localhost, for example.


### How to run me

#### Vol. 1 The Process Manager
Once you've followed the Install section, you can, on your host or on inside the container:
```bash
drunc-process-manager
```

And in another window (after `source setup.sh`), another container (if you've opened the correct port) or the same container as above:

```bash
drunc-process-manager-shell
```

You then end up in a shell on which you can execute commands like:
```
pm > boot data/controller-boot-many.json # Boot everything (top controller, controller and fake daq applications)
pm > ps                                  # List all the processes
pm > logs --uuid <UUID>                  # Stream the logs of a particular UUID (accessible above)
pm > kill --uuid <UUID>                  # Kill the process corresponding to the UUID
pm > restart --uuid <UUID>               # Restart the process corresponding to the UUID
pm > killall -u your_username            # Kill all your processes
pm > exit                                # When you've had enough
```
You can also do more complex things like selecting with partition etc.

Two important notes:
 - If you exit the `drunc-process-manager-shell`, the processes are _not_ killed, this is of course a desired feature.
 - Similarly, if you stop the `drunc-process-manager` (with `ctrl-C`), the processes are _not_ killed, I think we would want to kill everything here, but I'm not sure.

So, if like me, you start and stop every 2.5 minutes the process manager, first do `killall -f`, otherwise you'll have to use `htop` or similar tool to kill the processes manually.


#### Vol. 2 The Controller

By now you know how to spawn controllers and applications with the process manager. Now, as you know, the controllers are responsible for sending commands and retrieving status of their children (controllers or applications). This is still work in progress so check back here soon!
