import asyncio
from druncschema.request_response_pb2 import Request, Response, Description
from druncschema.process_manager_pb2 import BootRequest, ProcessUUID, ProcessQuery, ProcessInstance, ProcessInstanceList, ProcessMetadata, ProcessDescription, ProcessRestriction, LogRequest, LogLine

from drunc.utils.grpc_utils import unpack_any
from drunc.utils.shell_utils import GRPCDriver

from drunc.exceptions import DruncSetupException, DruncShellException


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


    async def _convert_oks_to_boot_request(
        self,
        oks_conf:str,
        user:str,
        session:str,
        override_logs:bool,
        connectivity_service_port:int=None) -> BootRequest:
        from drunc.process_manager.oks_parser import collect_apps, collect_infra_apps
        import conffwk
        from drunc.utils.configuration import find_configuration
        oks_conf = find_configuration(oks_conf)
        from logging import getLogger
        log = getLogger('_convert_oks_to_boot_request')
        log.info(oks_conf)


        db = conffwk.Configuration(f"oksconflibs:{oks_conf}")
        session_dal = db.get_dal(class_name="Session", uid=session)
        from drunc.process_manager.oks_parser import collect_variables
        env_throwaway = {}
        collect_variables(session_dal.environment, env_throwaway)

        if connectivity_service_port == 0 or not env_throwaway.get('CONNECTION_PORT'):
            from drunc.utils.utils import get_new_port
            connectivity_service_port = get_new_port()

        env = {
            'DUNEDAQ_PARTITION': session,
            'DUNEDAQ_SESSION': session,
            'DAQAPP_CLI_CONFIG_SVC': f"oksconflibs:{oks_conf}",
            'CONNECTION_PORT': str(connectivity_service_port) if connectivity_service_port is not None else None,
        }

        apps = collect_apps(db, session_dal, session_dal.segment, env)
        infra_apps = collect_infra_apps(session_dal, env)

        apps = infra_apps+apps

        def get_controller_address(top_controller_conf):
            service_id = top_controller_conf.id + "_control"
            port_number = None
            protocol = None
            for service in top_controller_conf.exposes_service:
                if service.id == service_id:
                    port_number = service.port
                    protocol = service.protocol
                    break
            if port_number is None or protocol is None:
                return None
            return f'{top_controller_conf.runs_on.runs_on.id}:{port_number}'

        self.controller_address = get_controller_address(session_dal.segment.controller)
        import json
        self._log.debug(f"{json.dumps(apps, indent=4)}")

        import os
        pwd = os.getcwd()

        for app in apps:
            host = app['restriction']
            name = app['name']
            exe = app['type']
            args = app['args']
            env = app['env']
            env['DUNE_DAQ_BASE_RELEASE'] = os.getenv("DUNE_DAQ_BASE_RELEASE")
            tree_id = app['tree_id']

            self._log.debug(f"{name}:\n{json.dumps(app, indent=4)}")
            executable_and_arguments = []

            if session_dal.rte_script:
                executable_and_arguments.append(ProcessDescription.ExecAndArgs(
                    exec='source',
                    args=[session_dal.rte_script]))

            elif os.getenv("DBT_INSTALL_DIR") is not None:
                env['DBT_INSTALL_DIR'] = os.getenv("DBT_INSTALL_DIR")
                self._log.info(f'RTE script was not supplied in the OKS configuration, using the one from local enviroment instead')
                rte = os.getenv("DBT_INSTALL_DIR") + "/daq_app_rte.sh"

                executable_and_arguments.append(ProcessDescription.ExecAndArgs(
                    exec='source',
                    args=[rte]))
            else:
                self._log.warning(f'RTE was not supplied in the OKS configuration or in the environment, running without it')

            # executable_and_arguments.append(
            #     ProcessDescription.ExecAndArgs(
            #         exec = "env",
            #         args = []
            #     )
            # )

            executable_and_arguments.append(ProcessDescription.ExecAndArgs(
                exec=exe,
                args=args))

            from drunc.utils.utils import now_str
            if override_logs:
                log_path = f'{pwd}/log_{user}_{session}_{name}.log'
            else:
                log_path = f'{pwd}/log_{user}_{session}_{name}_{now_str(True)}.log'
            self._log.debug(f'{name}\'s env:\n{env}')
            breq =  BootRequest(
                process_description = ProcessDescription(
                    metadata = ProcessMetadata(
                        user = user,
                        session = session,
                        name = name,
                        hostname = "",
                        tree_id = tree_id
                    ),
                    executable_and_arguments = executable_and_arguments,
                    env = env,
                    process_execution_directory = pwd,
                    process_logs_path = log_path,
                ),
                process_restriction = ProcessRestriction(
                    allowed_hosts = [host]
                )
            )
            self._log.debug(f"{breq=}\n\n")
            yield breq


    async def boot(self, conf:str, user:str, session_name:str, log_level:str, override_logs=True, **kwargs) -> ProcessInstance:

        async for br in self._convert_oks_to_boot_request(
            oks_conf = conf,
            user = user,
            session = session_name,
            override_logs = override_logs,
            **kwargs,
            ):
            yield await self.send_command_aio(
                'boot',
                data = br,
                outformat = ProcessInstance,
            )


    async def dummy_boot(self, user:str, session_name:str, n_processes:int, sleep:int, n_sleeps:int):# -> ProcessInstance:
        import os
        pwd = os.getcwd()

        # Construct the list of commands to send to the dummy_boot process
        executable_and_arguments = [ProcessDescription.ExecAndArgs(exec='echo',args=["Starting dummy_boot."])]
        for i in range(1,n_sleeps+1):
            executable_and_arguments += [ProcessDescription.ExecAndArgs(exec='sleep',args=[str(sleep)+"s"]), ProcessDescription.ExecAndArgs(exec='echo',args=[str(sleep*i)+"s"])]
        executable_and_arguments.append(ProcessDescription.ExecAndArgs(exec='echo',args=["Exiting."]))

        for process in range(n_processes):
            breq =  BootRequest(
                process_description = ProcessDescription(
                    metadata = ProcessMetadata(
                        user = user,
                        session = session_name,
                        name = "dummy_boot_"+str(process),
                        hostname = ""
                    ),
                    executable_and_arguments = executable_and_arguments,
                    env = {},
                    process_execution_directory = pwd,
                    process_logs_path = f'{pwd}/log_{user}_{session_name}_dummy-boot_'+str(process)+'.log',
                ),
                process_restriction = ProcessRestriction(
                    allowed_hosts = ["localhost"]
                )
            )
            self._log.debug(f"{breq=}\n\n")

            yield await self.send_command_aio(
                'boot',
                data = breq,
                outformat = ProcessInstance,
            )

    async def terminate(self, query:ProcessQuery) -> ProcessInstanceList:
        return await self.send_command_aio(
            'terminate',
            data = query,
            outformat = ProcessInstanceList,
        )

    async def kill(self, query:ProcessQuery) -> ProcessInstance:
        return await self.send_command_aio(
            'kill',
            data = query,
            outformat = ProcessInstanceList,
        )


    async def logs(self, req:LogRequest) -> LogLine:
        async for stream in self.send_command_for_aio(
            'logs',
            data = req,
            outformat = LogLine,
            ):
            yield stream


    async def ps(self, query:ProcessQuery) -> ProcessInstanceList:
        return await self.send_command_aio(
            'ps',
            data = query,
            outformat = ProcessInstanceList,
        )



    async def flush(self, query:ProcessQuery) -> ProcessInstanceList:
        return await self.send_command_aio(
            'flush',
            data = query,
            outformat = ProcessInstanceList,
        )


    async def restart(self, query:ProcessQuery) -> ProcessInstance:
        return await self.send_command_aio(
            'restart',
            data = query,
            outformat = ProcessInstance,
        )


    async def describe(self) -> Description:
        return await self.send_command_aio(
            'describe',
            outformat = Description,
        )
