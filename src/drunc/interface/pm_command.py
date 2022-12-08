import asyncio
import click
import grpc
import os

@click.command()
@click.option('--pm-conf', type=click.Path(exists=True), default=os.getenv('DRUNC_DATA')+'/process-manager.json', help='Where the process-manager configuration is')
def pm_command(pm_conf:str):
    from rich.console import Console
    console = Console()
    console.print(f'Using \'{pm_conf}\' as the ProcessManager configuration')
    pm_conf_data = {}
    
    with open(pm_conf) as f:
        import json
        pm_conf_data = json.loads(f.read())
    pm = None
    
    if pm_conf_data['type'] == 'ssh':
        from drunc.process_manager.ssh_process_manager import SSHProcessManager
        pm = SSHProcessManager()
    else:
        raise RuntimeError(f'ProcessManager type {pm_conf_data["type"]} is unsupported!')

    async def serve(address:str) -> None:
        if not address:
            raise RuntimeError('The address on which to expect commands/send status wasn\'t specified')
        from drunc.communication.process_manager_pb2_grpc import ProcessManagerServicer, add_ProcessManagerServicer_to_server
        server = grpc.aio.server()
        add_ProcessManagerServicer_to_server(pm, server)
        server.add_insecure_port(address)
        await server.start()
        console.print(f'ProcessManager was started on {address}')
        await server.wait_for_termination()
    try:
        asyncio.run(serve(pm_conf_data['address']))
    except Exception as e:
        console.print_exception()

