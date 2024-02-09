import asyncio
from druncschema.request_response_pb2 import Request, Response, Description
from druncschema.process_manager_pb2 import BootRequest, ProcessUUID, ProcessQuery, ProcessInstance, ProcessInstanceList, ProcessMetadata, ProcessDescription, ProcessRestriction, LogRequest, LogLine

from drunc.utils.grpc_utils import unpack_any
from drunc.utils.shell_utils import GRPCDriver

from drunc.exceptions import DruncSetupException, DruncShellException

class ConfigurationTypeNotSupported(DruncSetupException):
    def __init__(self, conf_type):
        self.type = conf_type
        super(ConfigurationTypeNotSupported, self).__init__(
            f'{str(conf_type)} is not supported by this process manager'
        )

class ProcessManagerDriver(GRPCDriver):
    controller_address = ''

    def __init__(self, address:str, token, **kwargs):
        super(ProcessManagerDriver, self).__init__(
            name = 'process_manager_driver',
            address = address,
            token = token,
            **kwargs
        )


    def create_stub(self, channel):
        from druncschema.process_manager_pb2_grpc import ProcessManagerStub
        return ProcessManagerStub(channel)


    async def _convert_boot_conf(self, conf, conf_type, user, session_name, log_level):
        from drunc.utils.configuration_utils import ConfTypes
        match conf_type:
            case ConfTypes.DAQConfDir:
                async for i in self._convert_daqconf_to_boot_request(conf, user, session_name, log_level):
                    yield i
            # case ConfTypes.DRUNC:
            #     async for i in self._convert_drunc_to_boot_request(conf, user, session_name):
            #         yield i
            case ConfTypes.OKSFileName:
                async for i in self._convert_oks_to_boot_request(conf, user, session_name):
                    yield i
            case _:
                raise ConfigurationTypeNotSupported(conf_type)


    async def _convert_daqconf_to_boot_request(self, daqconf_dir, user, session, loglevel) -> BootRequest:
        from logging import getLogger
        log = getLogger('_convert_daqconf_to_boot_request')
        from pathlib import Path
        boot_configuration = {}
        import os
        daqconf_fullpath_dir = Path(os.path.abspath(daqconf_dir))
        with open(Path(daqconf_fullpath_dir)/'boot.json') as f:
            import json
            boot_configuration = json.loads(f.read())

        env = boot_configuration['env']
        exec = boot_configuration['exec']
        rte = boot_configuration.get('rte_script')
        if rte is None:
            raise DruncShellException(f'RTE was not supplied in the boot.json')
        hosts = boot_configuration['hosts-ctrl']

        pwd = os.getcwd()

        from drunc.process_manager.boot_json_parser import process_exec, parse_configuration
        from drunc.utils.utils import now_str
        parsed_config_dir = Path(str(daqconf_fullpath_dir)+'_'+now_str(True))
        if boot_configuration['apps']:
            parse_configuration(
                input_dir = daqconf_fullpath_dir,
                output_dir = parsed_config_dir,
            )

        for svc_name, svc_data in boot_configuration.get('services', {}).items():
            raise DruncShellException('Services cannot be started by drunc (yet)')

        for app_name, app_data in boot_configuration['apps'].items():

            extra_args = []
            config = parsed_config_dir

            if 'drunc_controller' in app_data['exec']: # meh meh meh
                config = f'{parsed_config_dir}/controller.json'
                if loglevel:
                    extra_args = ['--log-level', loglevel]
                log.debug(f'{app_name=} (daq controller) {app_data=}')
                self.controller_address = f"{hosts[app_data['host']]}:{app_data['port']}"

            else:
                log.debug(f'{app_name=} (daq app) {app_data=}')


            br = process_exec(
                name = app_name,
                data = app_data,
                rte = rte,
                env = env,
                exec = exec,
                hosts = hosts,
                session = session,
                conf = f'file://{config}',
                pwd = pwd,
                user = user,
                extra_args = extra_args,
            )
            yield br


    async def _convert_oks_to_boot_request(self, oks_conf, user, session) -> BootRequest:
        from drunc.process_manager.oks_parser import process_segment
        import oksdbinterfaces
        db = oksdbinterfaces.Configuration("oksconfig:" + oks_conf)
        session_dal = db.get_dal(class_name="Session", uid=session)

        apps = process_segment(db, session_dal, session_dal.segment)
        self._log.debug(f"{apps=}")
        import os
        pwd = os.getcwd()

        ## Allocate ports for REST interface to apps
        ## Start with an arbitrary port for now
        #base_port = 9000
        #next_port = {}
        #for app in apps:
        #    if not host in next_port:
        #        port = base_port
        #    else:
        #        port = next_port[host]
        #    next_port[host] = port + 1
        #    app['port'] = port

        ##########  Kludge starts ################################
        args = apps[0]['args']
        from pathlib import Path
        from drunc.process_manager.boot_json_parser import process_exec, parse_configuration
        from drunc.utils.utils import now_str
        smatch = "file://"
        spos = args.find(smatch) + len(smatch)
        ematch = "controller.json"
        epos = args.find(ematch,spos) - 1
        cdir = args[spos:epos]
        pdir = cdir+'_'+now_str(True)
        config_dir = Path(cdir)
        parsed_config_dir = Path(pdir)
        parse_configuration(input_dir = config_dir,
                            output_dir = parsed_config_dir,
                            )
        # Load controller json, read ports for apps
        import json
        with open(pdir+'/controller.json') as f:
            ctl = json.loads(f.read())
            f.close()
        ports = {}
        for child in ctl["children"]:
            uri = child["uri"]
            port = uri[uri.find(":")+1:]
            ports[child["name"]] = port
            last_port = port
        for app in apps:
            if cdir+'/data' in app['args']:
                app['args'] = app['args'].replace(cdir+'/data', pdir)
                app['port'] = ports[app['name']]
            else:
                app['args'] = app['args'].replace(cdir, pdir)
                app['port'] = str(int(last_port) + 1)
        ##########  Kludge ends ################################

        #for name, exe, args, host, old_env in apps:
        for app in apps:
            host = app['restriction']
            name = app['name']
            port = app['port']
            exe = app['type']
            args = app['args']
            old_env = app['env']

            self._log.debug(f"{app=}")

            if 'drunc_controller' in exe: # meh meh meh
                self.controller_address = f"{host}:{port}"

            executable_and_arguments = []

            if session_dal.rte_script:
                executable_and_arguments.append(ProcessDescription.ExecAndArgs(
                    exec='source',
                    args=[session_dal.rte_script]))

            executable_and_arguments.append(ProcessDescription.ExecAndArgs(
                exec=exe,
                args=[args]))

            new_env = {
                "PORT": str(port),
            }
            for k, v in old_env.items():
                new_env[k] = v.format(**app)

            from drunc.utils.utils import now_str
            log_path = f'{pwd}/log_{user}_{session}_{name}_{now_str(True)}.log'

            self._log.debug(f"{new_env=}")
            breq =  BootRequest(
                process_description = ProcessDescription(
                    metadata = ProcessMetadata(
                        user = user,
                        session = session,
                        name = name,
                    ),
                    executable_and_arguments = executable_and_arguments,
                    env = new_env,
                    process_execution_directory = pwd,
                    process_logs_path = log_path,
                ),
                process_restriction = ProcessRestriction(
                    allowed_hosts = [host]
                )
            )
            self._log.debug(f"{breq=}\n\n")
            yield breq

    async def _convert_drunc_to_boot_request(self, boot_configuration_file, user, session) -> BootRequest:
        boot_configuration = {}
        with open(boot_configuration_file) as f:
            import json
            boot_configuration = json.loads(f.read())

        # For the future...
        # if not boot_configuration.is_valid():
        #     raise RuntimeError(f'Boot configuration isn\'t valid!')
        import os
        pwd = os.getcwd()

        for app in boot_configuration['instances']:

            executable_and_arguments = []
            for execargs in boot_configuration['executables'][app['type']]['executable_and_arguments']:
                for exe, args in execargs.items():
                    executable_and_arguments += [
                        ProcessDescription.ExecAndArgs(
                            exec=exe,
                            args=args
                        )
                    ]

            old_env = boot_configuration['executables'][app['type']]['environment']
            new_env = {
                'SESSION': session
            }
            for k, v in old_env.items():
                if v == 'getenv':
                    import os
                    var = os.getenv(k)
                    if var:
                        new_env[k] = var
                    else:
                        self._log.warning(f'Variable {k} is not in the environment, so won\'t be set.')

                else:
                    new_env[k] = v.format(**app) if isinstance(v, str) else str(v)

            from drunc.utils.utils import now_str
            log_path = f'{pwd}/log_{user}_{session}_{app["name"]}_{now_str(True)}.log'

            if 'topcontroller' in app['name']: # ARGGG
                host = boot_configuration['restrictions'][app['restriction']]['hosts'][0]
                self.controller_address = f"{host}:{app['port']}"

            yield BootRequest(
                process_description = ProcessDescription(
                    metadata = ProcessMetadata(
                        user = user,
                        session = session,
                        name = app['name'],
                    ),
                    executable_and_arguments = executable_and_arguments,
                    env = new_env,
                    process_execution_directory = pwd,
                    process_logs_path = log_path,
                ),
                process_restriction = ProcessRestriction(
                    allowed_hosts = boot_configuration['restrictions'][app['restriction']]['hosts']
                )
            )

    async def boot(self, conf:str, user:str, session_name:str, conf_type, log_level:str, rethrow=None) -> ProcessInstance:
        from drunc.exceptions import DruncShellException
        if rethrow is None:
            rethrow = self.rethrow_by_default

        try:
            async for br in self._convert_boot_conf(
                conf = conf,
                conf_type = conf_type,
                user = user,
                session_name = session_name,
                log_level = log_level):
                yield await self.send_command_aio(
                    'boot',
                    data = br,
                    outformat = ProcessInstance,
                    rethrow = rethrow,
                )
        except DruncShellException as e:
            if rethrow:
                raise e
            else:
                self._log.error(e)
                from drunc.utils.shell_utils import InterruptedCommand
                raise InterruptedCommand()


    async def kill(self, query:ProcessQuery, rethrow=None) -> ProcessInstance:
        return await self.send_command_aio(
            'kill',
            data = query,
            outformat = ProcessInstanceList,
            rethrow = rethrow,
        )


    async def logs(self, req:LogRequest, rethrow=None) -> LogLine:
        async for stream in self.send_command_for_aio(
            'logs',
            data = req,
            outformat = LogLine,
            rethrow = rethrow,):
            yield stream


    async def ps(self, query:ProcessQuery, rethrow=None) -> ProcessInstanceList:
        return await self.send_command_aio(
            'ps',
            data = query,
            outformat = ProcessInstanceList,
            rethrow = rethrow,
        )



    async def flush(self, query:ProcessQuery, rethrow=None) -> ProcessInstanceList:
        return await self.send_command_aio(
            'flush',
            data = query,
            outformat = ProcessInstanceList,
            rethrow = rethrow,
        )


    async def restart(self, query:ProcessQuery, rethrow=None) -> ProcessInstance:
        return await self.send_command_aio(
            'restart',
            data = query,
            outformat = ProcessInstance,
            rethrow = rethrow,
        )


    async def describe(self, rethrow=None) -> Description:
        return await self.send_command_aio(
            'describe',
            outformat = Description,
            rethrow = rethrow,
        )
