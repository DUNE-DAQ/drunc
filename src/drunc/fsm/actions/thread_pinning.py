from drunc.fsm.core import FSMAction
from drunc.utils.configuration import find_configuration
from drunc.fsm.exceptions import ThreadPinningFailed


class ThreadPinning(FSMAction):
    def __init__(self, configuration):
        super().__init__(
            name = "thread-pinning"
        )
        import logging
        self.log = logging.getLogger("thread-pinning")
        self.conf_dict = {p.name: p.value for p in configuration.parameters}

    def pin_thread(self, thread_pinning_file, configuration, session):
        from drunc.process_manager.oks_parser import collect_apps
        import conffwk
        db = conffwk.Configuration(f"oksconflibs:{configuration}")
        session_dal = db.get_dal(class_name="Session", uid=session)

        from os import environ
        env = environ.copy()

        apps = collect_apps(db, session_dal, session_dal.segment, environ)

        if session_dal.rte_script:
            rte = session_dal.rte_script

        else:
            from drunc.process_manager.utils import get_rte_script
            rte_script = get_rte_script()
            if not rte_script:
                raise DruncSetupException("No RTE script found.")

            rte = rte_script

        cmd = f"source {rte}; " if rte else ""
        cmd += f"readout-affinity.py --pinfile {thread_pinning_file}"

        import getpass
        user = getpass.getuser()

        hosts = set()
        for app in apps:
            hosts.add(app["host"])
        from sh import ssh, ErrorReturnCode, Command
        my_ssh = Command('/usr/bin/ssh')

        failed_hosts = set()

        for host in hosts:
            arguments = [user+"@"+host, "-tt", "-o StrictHostKeyChecking=no", f'{{ {cmd} ; }}']
            try:
                self.log.info(f"Executing '{cmd}'")
                self.log.info(f"Applying thread pinning {cmd} file {thread_pinning_file} on {host}")
                proc = my_ssh(*arguments)
            except ErrorReturnCode as e:
                self.log.error(e.stdout.decode('ascii'))
                self.log.error(e.stderr.decode('ascii'))
                failed_hosts.add(f'{host}: {e.stderr.decode("ascii")}')
                continue
            except Exception as e:
                self.log.critical(str(e))
                failed_hosts.add(f'{host}: {e}')
                continue
            self.log.debug(proc)

        failed_hosts_error_str = ", ".join(failed_hosts)
        if failed_hosts:
            raise ThreadPinningFailed(failed_hosts_error_str)

    def post_conf(self, _input_data, _context, **kwargs):
        run_configuration = find_configuration(_context.configuration.initial_data)
        if 'post_conf' in self.conf_dict:
            self.pin_thread(self.conf_dict['post_conf'], run_configuration, session=_context.session)
        return _input_data

    def post_start(self, _input_data, _context, **kwargs):
        run_configuration = find_configuration(_context.configuration.initial_data)
        if 'post_start' in self.conf_dict:
            self.pin_thread(self.conf_dict['post_start'], run_configuration, session=_context.session)
        return _input_data

    def pre_conf(self, _input_data, _context, **kwargs):
        run_configuration = find_configuration(_context.configuration.initial_data)
        if 'pre_conf' in self.conf_dict:
            self.pin_thread(self.conf_dict['pre_conf'], run_configuration, session=_context.session)
        return _input_data