import asyncio

from drunc.communication.process_manager_pb2 import BootRequest, ProcessUUID, ProcessInstance, ProcessInstanceList, ProcessMetadata, ProcessDescription, ProcessRestriction
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
        executable_and_arguments = []
        for execargs in boot_configuration['executable'][app_data['type']]['executable_and_arguments']:
            for exe, args in execargs.items():
                
        for app_name, app_data in boot_configuration['instances'].items():
            old_env = boot_configuration['executable'][app_data['type']]['environment']
            new_env = {}
            for k, v in old_env.items():
                new_env[k] = v.format(
                    NAME=app_name,
                    PORT=app_data['port'],
                    # ...?
                )
            br = BootRequest(
                process_description = ProcessDescription(
                    metadata = ProcessMetadata(
                        user = user,
                        partition = partition,
                        name = app_name,
                    ),
                    executable_and_arguments = {
                        exe: ProcessDescription.StringList(values = args)
                    },
                    env = new_env

                ),
                process_restriction = ProcessRestriction(
                    allowed_hosts = boot_configuration['restriction'][app_name]['hosts']
                )
            )
            yield await self.pm_stub.boot(br)

    async def kill(self, uuid:ProcessUUID) -> ProcessInstance:
        return await self.pm_stub.kill(uuid)

    async def list_process(self, selector:ProcessMetadata) -> ProcessInstanceList:
        return await self.pm_stub.list_process(selector)

    async def is_alive(self, selector:ProcessUUID) -> ProcessInstance:
        return await self.pm_stub.is_alive(selector)

    async def restart(self, selector:ProcessUUID) -> ProcessInstance:
        return await self.pm_stub.restart(selector)
