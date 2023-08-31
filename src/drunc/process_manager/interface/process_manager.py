import asyncio
import click
import grpc
import os

from drunc.utils.utils import log_levels

_cleanup_coroutines = []

@click.command()
@click.argument('pm-conf', type=click.Path(exists=True))
@click.option('-l', '--loglevel', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
def process_manager_cli(pm_conf:str, loglevel):
    from rich.console import Console
    console = Console()
    console.print(f'Using \'{pm_conf}\' as the ProcessManager configuration')
    pm_conf_data = None
    from drunc.utils.utils import update_log_level
    update_log_level(loglevel)

    with open(pm_conf) as f:
        import json
        pm_conf_data = json.loads(f.read())

    from drunc.process_manager.process_manager import ProcessManager
    pm = ProcessManager.get(pm_conf_data)
    loop = asyncio.get_event_loop()

    async def serve(address:str) -> None:
        if not address:
            raise RuntimeError('The address on which to expect commands/send status wasn\'t specified')
        from druncschema.process_manager_pb2_grpc import add_ProcessManagerServicer_to_server

        server = grpc.aio.server()
        add_ProcessManagerServicer_to_server(pm, server)
        server.add_insecure_port(address)

        await server.start()

        console.print(f'ProcessManager was started on {address}')

        async def server_shutdown():
            console.print("Starting shutdown...")
            # Shuts down the server with 5 seconds of grace period. During the
            # grace period, the server won't accept new connections and allow
            # existing RPCs to continue within the grace period.
            await server.stop(5)
            pm.terminate()

        _cleanup_coroutines.append(server_shutdown())
        await server.wait_for_termination()


    try:
        loop.run_until_complete(
            serve(pm_conf_data['command_address'])
        )
    except Exception as e:
        import os
        console.print_exception(width=os.get_terminal_size()[0])
    finally:
        loop.run_until_complete(*_cleanup_coroutines)
        loop.close()