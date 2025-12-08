import getpass
from os import environ

import conffwk
from sh import Command, ErrorReturnCode

from drunc.exceptions import DruncSetupException
from drunc.fsm.core import FSMAction
from drunc.fsm.exceptions import ThreadPinningFailed
from drunc.process_manager.oks_parser import collect_apps
from drunc.process_manager.utils import get_rte_script
from drunc.utils.utils import get_logger


class ThreadPinning(FSMAction):
    def __init__(self, configuration):
        super().__init__(name="thread-pinning")
        self.log = get_logger(
            "controller.iface.thread-pinning"
        )  # TODO: Verify core/interface choice
        self.conf_dict = {p.name: p.value for p in configuration.parameters}

    def pin_thread(self, thread_pinning_file, configuration, session):
        db = conffwk.Configuration(configuration)
        session_dal = db.get_dal(class_name="Session", uid=session)

        apps = collect_apps(
            config_filename=configuration,
            session_name=session,
            db=db,
            session_obj=session_dal,
            segment_obj=session_dal.segment,
            env=environ,
            tree_prefix=[],
        )

        if session_dal.rte_script:
            rte = session_dal.rte_script

        else:
            rte_script = get_rte_script()
            if not rte_script:
                raise DruncSetupException("No RTE script found.")

            rte = rte_script

        cmd = f"source {rte}; " if rte else ""
        cmd += f"readout-affinity.py --pinfile {thread_pinning_file}"

        user = getpass.getuser()

        hosts = set()
        for app in apps:
            hosts.add(app["host"])

        my_ssh = Command("/usr/bin/ssh")

        failed_hosts = set()

        for host in hosts:
            arguments = [
                user + "@" + host,
                "-tt",
                "-o StrictHostKeyChecking=no",
                f"{{ {cmd} ; }}",
            ]
            try:
                self.log.info(f"Executing '{cmd}'")
                self.log.info(
                    f"Applying thread pinning {cmd} file {thread_pinning_file} on {host}"
                )
                proc = my_ssh(
                    *arguments,
                    _err_to_out=True,
                )
                self.log.info(proc)
            except ErrorReturnCode as e:
                self.log.error(e.stdout.decode("ascii"))
                self.log.error(e.stderr.decode("ascii"))
                failed_hosts.add(f"{host}: {e.stderr.decode('ascii')}")
                continue
            except Exception as e:
                self.log.critical(str(e))
                failed_hosts.add(f"{host}: {e}")
                continue
            self.log.debug(proc)

        failed_hosts_error_str = ", ".join(failed_hosts)
        if failed_hosts:
            raise ThreadPinningFailed(failed_hosts_error_str)

    def post_conf(self, _input_data, _context, **kwargs):
        if "post_conf" in self.conf_dict:
            self.pin_thread(
                self.conf_dict["post_conf"],
                _context.configuration.initial_data,
                session=_context.configuration.oks_key.session,
            )
        return _input_data

    def post_start(self, _input_data, _context, **kwargs):
        if "post_start" in self.conf_dict:
            self.pin_thread(
                self.conf_dict["post_start"],
                _context.configuration.initial_data,
                session=_context.configuration.oks_key.session,
            )
        return _input_data

    def pre_conf(self, _input_data, _context, **kwargs):
        if "pre_conf" in self.conf_dict:
            self.pin_thread(
                self.conf_dict["pre_conf"],
                _context.configuration.initial_data,
                session=_context.configuration.oks_key.session,
            )
        return _input_data
