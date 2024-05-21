# DUNE Run Control (drunc)

This is a skeleton of a run control to be used in DUNE. The project is still in its "fundation stage", which basically means it doesn't work for other than experimental purposes.

## Install

### Requirements
This python code uses `rich`, `gRPC`. The installation of these is handled automatically by the installation script.

### How to install me on your machine
```bash
git clone git@github.com:DUNE-DAQ/drunc.git -b develop      # clone the latest verified working version of the code
source setup.sh                                             # setup the env variable DRUNC_DATA
pip install .                                               # ... install this package if you are planning to modify you can use `pip install -e .`
```

Next time you log in, set all the required environment variables from your working directory as
```bash
source setup.sh
```

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


## Run me
See [https://github.com/DUNE-DAQ/drunc/wiki/Running-drunc](url) for guidance.
