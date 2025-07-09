import signal
import os
import time
import subprocess
import click

def terminate_all(sig, frame):
    print('')
    pgrp = os.getpgid(os.getpid())
    os.killpg(pgrp, signal.SIGKILL)


@click.command()
@click.argument("cmd")
@click.option("-l", "--log", "log_path", type=click.Path(file_okay=True, dir_okay=False), required=True)
def main(cmd: str, log_path: str):
    signal.signal(signal.SIGHUP, terminate_all)

    with open(log_path, 'w') as logfile:
        proc = subprocess.Popen(cmd, shell=True, stdout=logfile, stderr=logfile)
        print(f"Started process with pid {proc.pid}")

        return_code = None
        while True:
            return_code = proc.poll()
            if return_code is not None:
                break
            time.sleep(0.1)


if __name__ == '__main__':
    main()
