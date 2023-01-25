import click

from drunc.utils.utils import CONTEXT_SETTINGS, log_levels,  update_log_level

@click.command()
@click.argument('configuration', type=str)
@click.argument('port', type=int)
@click.argument('name', type=str)
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
def controller_cli(configuration:str, port:int, name:str, log_level:str):
    from rich.console import Console
    console = Console()
    # console.print(f'Using \'{pm_conf}\' as the ProcessManager configuration')
    
    update_log_level(log_level)
    
    from drunc.controller.controller import Controller
    from drunc.communication.controller_pb2_grpc import ControllerServicer, add_ControllerServicer_to_server
    import asyncio
    import grpc

    ctrlr = Controller(name, configuration)

    async def serve(port:int) -> None:
        if not port:
            raise RuntimeError('The port on which to expect commands/send status wasn\'t specified')
        server = grpc.aio.server()
        add_ControllerServicer_to_server(ctrlr, server)
        listen_addr = f'[::]:{port}'
        server.add_insecure_port(listen_addr)
        await server.start()
        console.print(f'{ctrlr.name} was started on {listen_addr}')
        await server.wait_for_termination()

    try:
        asyncio.run(serve(port))
    except Exception as e:
        console.print_exception()

