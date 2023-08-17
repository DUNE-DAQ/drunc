import click
import signal
import sys
from drunc.utils.utils import CONTEXT_SETTINGS, log_levels,  update_log_level

@click.command()
@click.argument('configuration', type=str)
@click.argument('control-port', type=int)
@click.argument('name', type=str)
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
def controller_cli(configuration:str, control_port:int, name:str, log_level:str):
    from rich.console import Console
    console = Console()

    update_log_level(log_level)

    from drunc.controller.controller import Controller
    from druncschema.controller_pb2_grpc import add_ControllerServicer_to_server
    import grpc

    ctrlr = Controller(name, configuration)

    def serve(port:int) -> None:
        if not port:
            raise RuntimeError('The port on which to expect commands/send status wasn\'t specified')

        from concurrent import futures
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))

        add_ControllerServicer_to_server(ctrlr, server)

        listen_addr = f'[::]:{port}'
        server.add_insecure_port(listen_addr)

        server.start()
        console.print(f'{ctrlr.name} was started on {listen_addr}')

        def sigint(sig, frame):
            console.print('Requested termination')
            server.stop(0)
            console.print('Server stopped')
            ctrlr.stop()
            console.print('Controller stopped')

        signal.signal(signal.SIGINT, sigint)

        server.wait_for_termination()
        console.print(f'{ctrlr.name} was terminated')

    try:
        serve(control_port)
    except Exception as e:
        console.print_exception()
