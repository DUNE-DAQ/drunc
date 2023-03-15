import asyncio

from drunc.communication.process_manager_pb2 import BootRequest, ProcessUUID, ProcessQuery, ProcessInstance, ProcessInstanceList, ProcessMetadata, ProcessDescription, ProcessRestriction, LogRequest, LogLine
from drunc.communication.process_manager_pb2_grpc import ProcessManagerStub


class ProcessManagerDriver:
    def __init__(self, pm_conf:dict):
        import grpc
        self.pm_address = pm_conf['address']
        self.pm_channel = grpc.aio.insecure_channel(self.pm_address)
        self.pm_stub = ProcessManagerStub(self.pm_channel)

    async def boot(self, boot_configuration_file:str, user:str, partition:str) -> ProcessUUID:
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

            br = BootRequest(
                process_description = ProcessDescription(
                    metadata = ProcessMetadata(
                        user = user,
                        partition = partition,
                        name = app['name'],
                    ),
                    executable_and_arguments = executable_and_arguments,
                    env = new_env

                ),
                process_restriction = ProcessRestriction(
                    allowed_hosts = boot_configuration['restrictions'][app['restriction']]['hosts']
                )
            )
            yield await self.pm_stub.boot(br)

    async def kill(self, query:ProcessQuery) -> ProcessInstance:
        return await self.pm_stub.kill(query)

    async def killall(self, query:ProcessQuery) -> ProcessInstanceList:
        return await self.pm_stub.killall(query)

    async def logs(self, req:LogRequest) -> LogLine:
        async for ll in self.pm_stub.logs(req):
            yield ll

    async def list_process(self, query:ProcessQuery) -> ProcessInstanceList:
        return await self.pm_stub.list_process(query)

    async def flush(self, query:ProcessQuery) -> ProcessInstanceList:
        return await self.pm_stub.flush(query)

    async def is_alive(self, query:ProcessQuery) -> ProcessInstance:
        return await self.pm_stub.is_alive(query)

    async def restart(self, query:ProcessQuery) -> ProcessInstance:
        return await self.pm_stub.restart(query)
