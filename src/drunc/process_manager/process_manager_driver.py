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


    async def _convert_oks_to_boot_request(self, oks_conf, user, session, override_logs) -> BootRequest:
        from drunc.process_manager.oks_parser import process_segment
        import conffwk
        from drunc.utils.configuration import find_configuration
        oks_conf = find_configuration(oks_conf)
        from logging import getLogger
        log = getLogger('_convert_oks_to_boot_request')
        log.info(oks_conf)
        db = conffwk.Configuration(f"oksconflibs:{oks_conf}")
        session_dal = db.get_dal(class_name="Session", uid=session)

        apps = process_segment(db, session_dal, session_dal.segment)

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
        self._log.debug(f"{apps=}")
        import os
        pwd = os.getcwd()

        for app in apps:
            host = app['restriction']
            name = app['name']
            exe = app['type']
            args = app['args']
            env = app['env']

            self._log.debug(f"{app=}")

            executable_and_arguments = []

            if session_dal.rte_script:
                executable_and_arguments.append(ProcessDescription.ExecAndArgs(
                    exec='source',
                    args=[session_dal.rte_script]))

            elif os.getenv("DBT_INSTALL_DIR") is not None:
                self._log.info(f'RTE script was not supplied in the OKS configuration, using the one from local enviroment instead')
                rte = os.getenv("DBT_INSTALL_DIR") + "/daq_app_rte.sh"

                executable_and_arguments.append(ProcessDescription.ExecAndArgs(
                    exec='source',
                    args=[rte]))
            else:
                self._log.warning(f'RTE was not supplied in the OKS configuration or in the environment, running without it')

            executable_and_arguments.append(
                ProcessDescription.ExecAndArgs(
                    exec = "env",
                    args = []
                )
            )

            executable_and_arguments.append(ProcessDescription.ExecAndArgs(
                exec=exe,
                args=args))


            from drunc.utils.utils import now_str
            if override_logs:
                log_path = f'{pwd}/log_{user}_{session}_{name}.log'
            else:
                log_path = f'{pwd}/log_{user}_{session}_{name}_{now_str(True)}.log'

            breq =  BootRequest(
                process_description = ProcessDescription(
                    metadata = ProcessMetadata(
                        user = user,
                        session = session,
                        name = name,
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


    async def boot(self, conf:str, user:str, session_name:str, log_level:str, rethrow=None, override_logs=True) -> ProcessInstance:
        from drunc.exceptions import DruncShellException
        if rethrow is None:
            rethrow = self.rethrow_by_default

        try:
            async for br in self._convert_oks_to_boot_request(
                oks_conf = conf,
                user = user,
                session = session_name,
                # log_level = log_level
                override_logs = override_logs,
                ):
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
