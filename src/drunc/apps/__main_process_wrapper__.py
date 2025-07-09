import os
import signal
import subprocess
import time

import click

from drunc.process_manager.popen_process_manager import on_parent_exit


def terminate_all(sig, frame):
    pgrp = os.getpgid(os.getpid())
    os.killpg(pgrp, signal.SIGKILL)


@click.command()
@click.option(
    "-l",
    "--log",
    "log_path",
    type=click.Path(file_okay=True, dir_okay=False),
    required=True,
)
@click.argument("cmd")
def main(cmd: str, log_path: str):
    signal.signal(signal.SIGHUP, terminate_all)
    signal.signal(signal.SIGINT, terminate_all)

    with open(log_path, "w") as logfile:
        proc = subprocess.Popen(
            cmd,
            shell=True,
            stdout=logfile,
            stderr=logfile,
            preexec_fn=on_parent_exit(
                signal.SIGHUP,  # Propagate SIGHUP to child processes, SIGKILL doesn't seem to kill gunicorn...
                setsid=False,  # Don't create a new session, so that the process group can be killed
            ),
        )

        return_code = None
        while True:
            return_code = proc.poll()
            if return_code is not None:
                return return_code
            time.sleep(0.1)


if __name__ == "__main__":
    main()
