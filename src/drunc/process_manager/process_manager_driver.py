import asyncio
from druncschema.request_response_pb2 import Request, Response, Description
from druncschema.process_manager_pb2 import BootRequest, ProcessUUID, ProcessQuery, ProcessInstance, ProcessInstanceList, ProcessMetadata, ProcessDescription, ProcessRestriction, LogRequest, LogLine
from druncschema.process_manager_pb2_grpc import ProcessManagerStub
from druncschema.token_pb2 import Token
from google.protobuf.any_pb2 import Any
from drunc.utils.grpc_utils import unpack_any

class ConfigurationTypeNotSupported(Exception):
    def __init__(self, conf_type):
        self.type = conf_type
        super(ConfigurationTypeNotSupported, self).__init__(
            f'{str(conf_type)} is not supported by this process manager'
        )

class ProcessManagerDriver:
    def __init__(self, address:str, token):
        import logging
        self._log = logging.getLogger('ProcessManagerDriver')
        import grpc
        self.token = Token()
        self.token.CopyFrom(token)
        self.pm_address = address
        self.pm_channel = grpc.aio.insecure_channel(self.pm_address)
        self.pm_stub = ProcessManagerStub(self.pm_channel)


    def _create_request(self, payload=None):
        token = Token()
        token.CopyFrom(self.token)
        data = Any()
        if payload is not None:
            data.Pack(payload)

        if payload:
            return Request(
                token = token,
                data = data
            )
        else:
            return Request(
                token = token
            )


    async def _convert_boot_conf(self, conf, conf_type, user, session_name, log_level):
        from drunc.process_manager.utils import ConfTypes
        match conf_type:
            case ConfTypes.DAQCONF:
                async for i in self._convert_daqconf_to_boot_request(conf, user, session_name, log_level):
                    yield i
            case ConfTypes.DRUNC:
                async for i in self._convert_drunc_to_boot_request(conf, user, session_name):
                    yield i
            case ConfTypes.OKS:
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
            raise RuntimeError(f'RTE was not supplied in the boot.json')
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
            raise RuntimeError('Services cannot be started by drunc (yet)')

        children_app = []

        for app_name, app_data in boot_configuration['apps'].items():
            children_app += [{
                'name': app_name,
                'uri': f'{hosts[app_name]}:{app_data["port"]}',
                'type': 'rest-api'
            }]

            extra_args = []
            config = parsed_config_dir
            if 'drunc_controller' in app_data['exec']: # meh meh meh
                config = f'{parsed_config_dir}/controller.json'
                if loglevel:
                    extra_args = ['--log-level', loglevel]
                log.debug(f'{app_name=} (daq controller) {app_data=}')
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

        # Start with an arbitrary port for now
        base_port = 9000
        next_port = {}
        firstApp = True
        #for name, exe, args, host, old_env in apps:
        for app in apps:
            host = app['restriction']
            name = app['name']
            exe = app['type']
            args = app['args']
            old_env = app['env']
            if not host in next_port:
                port = base_port
            else:
                port = next_port[host]
            next_port[host] = port + 1
            app['port'] = port

            if firstApp:
                from pathlib import Path
                from drunc.process_manager.boot_json_parser import process_exec, parse_configuration
                from drunc.utils.utils import now_str
                smatch = "file://"
                spos = args.find(smatch) + len(smatch)
                ematch = "/data"
                if not ematch in args[spos:]:
                    ematch = "controller.json"
                epos = args.find(ematch,spos) - 1
                cdir = args[spos:epos]
                pdir = cdir+'_'+now_str(True)
                config_dir = Path(cdir)
                parsed_config_dir = Path(pdir)
                parse_configuration(input_dir = config_dir,
                                    output_dir = parsed_config_dir,
                                    )
                firstApp = False
            if cdir+'/data' in args:
                args = args.replace(cdir+'/data', pdir)
            else:
                args = args.replace(cdir, pdir)

            self._log.debug(f"{app=}")

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

    async def boot(self, conf:str, user:str, session_name:str, conf_type, log_level:str) -> ProcessInstance:
        async for br in self._convert_boot_conf(
            conf = conf,
            conf_type = conf_type,
            user = user,
            session_name = session_name,
            log_level = log_level):
            answer = await self.pm_stub.boot(
                self._create_request(br)
            )
            pd = unpack_any(answer.data,ProcessInstance)
            yield pd


    async def kill(self, query:ProcessQuery) -> ProcessInstance:
        answer = await self.pm_stub.kill(
            self._create_request(payload = query)
        )
        pi = unpack_any(answer.data, ProcessInstanceList)
        return pi


    async def logs(self, req:LogRequest) -> LogLine:
        async for stream in self.pm_stub.logs(self._create_request(payload = req)):
            ll = unpack_any(stream.data, LogLine)
            yield ll

    async def ps(self, query:ProcessQuery) -> ProcessInstanceList:
        answer = await self.pm_stub.ps(
            self._create_request(payload = query)
        )
        pil = unpack_any(answer.data, ProcessInstanceList)
        return pil

    async def flush(self, query:ProcessQuery) -> ProcessInstanceList:
        answer = await self.pm_stub.flush(
            self._create_request(payload = query)
        )
        pil = unpack_any(answer.data, ProcessInstanceList)
        return pil

    async def restart(self, query:ProcessQuery) -> ProcessInstance:
        answer = await self.pm_stub.restart(
            self._create_request(payload = query)
        )
        pi = unpack_any(answer.data, ProcessInstance)
        return pi

    async def describe(self) -> Description:
        r = self._create_request(payload = None)
        answer = await self.pm_stub.describe(
            r
        )
        desc = unpack_any(answer.data, Description)
        return desc
