import argparse
import conffwk
import getpass
import signal
import os
from sh import Command

from drunc.utils.configuration import find_configuration
from drunc.utils.utils import expand_path
from drunc.process_manager.ssh_process_manager import on_parent_exit

def validate_config(confiuguration_filename:str, session_name:str):
    conf_with_dir = find_configuration(confiuguration_filename)
    db = conffwk.Configuration(f"oksconflibs:{conf_with_dir}")
    session_dal = db.get_dal(class_name="Session", uid=session_name)
    assert session_dal.segment.controller.id == "root-controller"
    hosts = set([])
    platform = os.uname().sysname.lower()
    macos = ("darwin" in platform)

    for segment in session_dal.segment.segments: # For each segment in the session
        for segment_application in segment.applications: # For each application in the segment
            if segment_application not in session_dal.disabled: # If it is not disabled
                hosts.add(segment_application.runs_on.runs_on.id)

    hosts.add("abc12") # case to throw, not throwing as expected
    ssh = Command('/usr/bin/ssh')
    runnning_tunnels = []
    failed_hosts = []

    for host in hosts:
        user_host = f"{getpass.getuser()}@{host}"
        ssh_args = [user_host, "sleep 2s; exit;"]
        try:
            runnning_tunnels.append(
                ssh(
                    *ssh_args,
                    _bg=True,
                    _bg_exc=True,
                    _new_session=True,
                    _preexec_fn = on_parent_exit(signal.SIGTERM) if not macos else None
                )
            )
        except sh.ErrorReturnCode_255:
            print(f"Failed to SSH onto host {user_host}")
            failed_hosts.append(host)
            continue

    for tunnel in runnning_tunnels:
        tunnel.wait(timeout=10)

    if (len(failed_hosts) != 0):
        print(f"Failed to connect to hosts {failed_hosts}")
    else:
        print(f"Connected to hosts {hosts} successfully")

def main():
    parser = argparse.ArgumentParser(
        prog = "drunc-ssh-validator",
        description = "Verifies ssh access to all the hosts required by the session <session> defined in configuration <configuration_filename>"
    )
    parser.add_argument("configuration_filename", help="Name of the configuration file to verify. Note this is not for the process manager.")
    parser.add_argument("session", help="Name of the session to test")
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    validate_config(args.configuration_filename, args.session)

if __name__ == '__main__':
    main()