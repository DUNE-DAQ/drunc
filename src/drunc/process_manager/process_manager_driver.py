import asyncio
from druncschema.request_response_pb2 import Request, Response
from druncschema.process_manager_pb2 import BootRequest, ProcessUUID, ProcessQuery, ProcessInstance, ProcessInstanceList, ProcessMetadata, ProcessDescription, ProcessRestriction, LogRequest, LogLine
from druncschema.process_manager_pb2_grpc import ProcessManagerStub
from druncschema.token_pb2 import Token
from google.protobuf.any_pb2 import Any
from drunc.utils.grpc_utils import unpack_any

class ConfigurationTypeNotSupported(Exception):
    def __init__(self, conf_type):
        self.type = conf_type
        super().__init__(f'{str(conf_type)} is not supported by this process manager')

class ProcessManagerDriver:
    def __init__(self, pm_conf:dict, token):
        import grpc
        self.token = Token()
        self.token.CopyFrom(token)
        self.pm_address = pm_conf['address']
        self.pm_channel = grpc.aio.insecure_channel(self.pm_address)
        self.pm_stub = ProcessManagerStub(self.pm_channel)


    def _create_request(self, payload=None):
        token = Token()
        token.CopyFrom(self.token)
        data = Any()
        if payload:
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


    async def _convert_boot_conf(self, conf, conf_type, user, session):
        from drunc.process_manager.utils import ConfTypes
        match conf_type:
            case ConfTypes.DAQCONF:
                async for i in self._convert_daqconf_to_boot_request(conf, user, session):
                    yield i
            case ConfTypes.DRUNC:
                async for i in self._convert_drunc_to_boot_request(conf, user, session):
                    yield i
            case ConfTypes.OKS:
                async for i in self._convert_oks_to_boot_request(conf, user, session):
                    yield i
            case _:
                raise ConfigurationTypeNotSupported(conf_type)

    async def _convert_daqconf_to_boot_request(self, daqconf_dir, user, session) -> BootRequest:
        from drunc.process_manager.utils import ConfTypes
        raise ConfigurationTypeNotSupported(ConfTypes.DAQCONF)
        yield None

    async def _convert_oks_to_boot_request(self, oks_conf, user, session) -> BootRequest:
        from drunc.process_manager.utils import ConfTypes
        raise ConfigurationTypeNotSupported(ConfTypes.OKS)
        yield None

    async def _convert_drunc_to_boot_request(self, boot_configuration_file, user, session) -> BootRequest:
        boot_configuration = {}
        with open(boot_configuration_file) as f:
            import json
            boot_configuration = json.loads(f.read())

        # For the future...
        # if not boot_configuration.is_valid():
        #     raise RuntimeError(f'Boot configuration isn\'t valid!')

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
            new_env = {}
            for k, v in old_env.items():
                if v == 'getenv':
                    import os
                    try:
                        new_env[k] = os.getenv(k)
                    except:
                        print(f'Variable {k} is not in the environment, so won\'t be set.')
                else:
                    new_env[k] = v.format(**app)

            yield BootRequest(
                process_description = ProcessDescription(
                    metadata = ProcessMetadata(
                        user = user,
                        session = session,
                        name = app['name'],
                    ),
                    executable_and_arguments = executable_and_arguments,
                    env = new_env
                ),
                process_restriction = ProcessRestriction(
                    allowed_hosts = boot_configuration['restrictions'][app['restriction']]['hosts']
                )
            )

    async def boot(self, boot_configuration:str, user:str, session:str, conf_type) -> ProcessInstance:
        async for br in self._convert_boot_conf(boot_configuration, conf_type, user, session):
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
