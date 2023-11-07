import asyncio
import click
import grpc
import os

from drunc.utils.utils import log_levels

_cleanup_coroutines = []

@click.command()
@click.argument('sm-conf', type=click.Path(exists=True))
@click.option('-l', '--loglevel', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
def session_manager_cli(sm_conf:str, loglevel):
    from rich.console import Console
    console = Console()
    console.print(f'Using \'{sm_conf}\' as the SessionManager configuration')
    sm_conf_data = None
    from drunc.utils.utils import update_log_level
    update_log_level(loglevel)

    with open(sm_conf) as f:
        import json
        sm_conf_data = json.loads(f.read())

    from drunc.session_manager.session_manager import SessionManager
    pm = SessionManager.get(sm_conf_data, name='session_manager')
    loop = asyncio.get_event_loop()

    async def serve(address:str) -> None:
        if not address:
            raise RuntimeError('The address on which to expect commands/send status wasn\'t specified')
        from druncschema.session_manager_pb2_grpc import add_SessionManagerServicer_to_server

        server = grpc.aio.server()
        add_SessionManagerServicer_to_server(pm, server)
        server.add_insecure_port(address)

        await server.start()

        console.print(f'SessionManager was started on {address}')

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
            serve(sm_conf_data['command_address'])
        )
    except Exception as e:
        import os
        console.print_exception(width=os.get_terminal_size()[0])
    finally:
        if _cleanup_coroutines:
            loop.run_until_complete(*_cleanup_coroutines)
        loop.close()