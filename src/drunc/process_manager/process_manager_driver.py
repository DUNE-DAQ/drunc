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



    def generate_app_controller_conf(self, children, output_file):
        data = {
            "children": children,

            "broadcaster": {
                "type": "kafka",
                "kafka_address": "monkafka.cern.ch:30092",
                "publish_timeout": 2
            },

            "statefulnode": {
                "included": True,
                "fsm":{
                    "states": [
                        "initial", "configured", "ready", "running",
                        "paused", "dataflow_drained", "trigger_sources_stopped", "error"
                    ],
                    "initial_state": "initial",
                    "transitions": [
                        { "trigger": "conf",                 "source": "initial",                 "dest": "configured"             },
                        { "trigger": "start",                "source": "configured",              "dest": "ready"                  },
                        { "trigger": "enable_triggers",      "source": "ready",                   "dest": "running"                },
                        { "trigger": "disable_triggers",     "source": "running",                 "dest": "ready"                  },
                        { "trigger": "drain_dataflow",       "source": "ready",                   "dest": "dataflow_drained"       },
                        { "trigger": "stop_trigger_sources", "source": "dataflow_drained",        "dest": "trigger_sources_stopped"},
                        { "trigger": "stop",                 "source": "trigger_sources_stopped", "dest": "configured"             },
                        { "trigger": "scrap",                "source": "configured",              "dest": "initial"                }
                    ],
                    "command_sequences": {
                        "start_run": [
                            {"cmd": "conf",            "optional": True },
                            {"cmd": "start",           "optional": False},
                            {"cmd": "enable_triggers", "optional": False}
                        ],
                        "stop_run" : [
                            {"cmd": "disable_triggers",     "optional": True },
                            {"cmd": "drain_dataflow",       "optional": False},
                            {"cmd": "stop_trigger_sources", "optional": False},
                            {"cmd": "stop",                 "optional": False}
                        ],
                        "shutdown" : [
                            {"cmd": "disable_triggers",     "optional": True },
                            {"cmd": "drain_dataflow",       "optional": True },
                            {"cmd": "stop_trigger_sources", "optional": True },
                            {"cmd": "stop",                 "optional": True },
                            {"cmd": "scrap",                "optional": True }
                        ]
                    },
                    "interfaces": {
                        "user-provided-run-number": {},
                    },
                    "pre_transitions": {
                        "start":  {"order": ["user-provided-run-number"], "mandatory": ["user-provided-run-number"]}
                    },
                    "post_transitions": {}
                }
            }
        }
        import json
        json.dump(data, open(output_file, 'w'), indent=4)



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
            log.debug(f'{app_name=}, {app_data=}')
            br = process_exec(
                name = app_name,
                data = app_data,
                rte = rte,
                env = env,
                exec = exec,
                hosts = hosts,
                session = session,
                conf = f'file://{parsed_config_dir}',
                pwd = pwd,
                user = user,
            )
            yield br

        ctrler_conf = parsed_config_dir/'controller.json'

        self.generate_app_controller_conf(
            children = children_app,
            output_file = ctrler_conf,
        )

        executable_and_arguments = [
            ProcessDescription.ExecAndArgs(
                exec = 'source',
                args = ["${DBT_INSTALL_DIR}/daq_app_rte.sh"]
            ),
            ProcessDescription.ExecAndArgs(
                exec = 'drunc-controller',
                args = [
                    "${CONFIGURATION}",
                    "${PORT}",
                    "${NAME}",
                    "${SESSION}",
                    "--log-level",
                    loglevel,
                ]
            )
        ]

        ctrler_env = {
            'SESSION': session,
            "CONFIGURATION": f"file://{str(ctrler_conf)}",
            "DBT_INSTALL_DIR": "getenv",
            "NAME": "topcontroller",
            "PORT": "3600",
            "COLUMNS": 150
        }

        from drunc.process_manager.boot_json_parser import process_env
        ctrler_env = process_env(ctrler_env, rte is not None)

        from drunc.utils.utils import now_str
        log_path = f'{pwd}/log_{user}_{session}_{ctrler_env["NAME"]}_{now_str(True)}.log'

        yield BootRequest(
            process_description = ProcessDescription(
                metadata = ProcessMetadata(
                    user = user,
                    session = session,
                    name = ctrler_env['NAME'],
                ),
                executable_and_arguments = executable_and_arguments,
                env = ctrler_env,
                process_execution_directory = pwd,
                process_logs_path = log_path,
            ),
            process_restriction = ProcessRestriction(
                allowed_hosts = ['localhost']
            )
        )



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
