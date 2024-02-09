import click
import signal
import sys
from drunc.utils.utils import log_levels,  update_log_level

@click.command()
@click.argument('configuration', type=str)
@click.argument('control-port', type=int)
@click.argument('name', type=str)
@click.argument('session', type=str)
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
def controller_cli(configuration:str, control_port:int, name:str, session:str, log_level:str):

    from rich.console import Console
    console = Console()

    update_log_level(log_level)

    from drunc.controller.controller import Controller
    from druncschema.controller_pb2_grpc import add_ControllerServicer_to_server
    import grpc

    from drunc.utils.configuration_utils import ConfData
    configuration_data = ConfData.get_from_url(configuration)

    ctrlr = Controller(
        name = name,
        session = session,
        configuration = configuration_data
    )

    def serve(port:int) -> None:
        if not port:
            from drunc.exceptions import DruncSetupException
            raise DruncSetupException('The port on which to expect commands/send status wasn\'t specified')

        from concurrent import futures
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))

        add_ControllerServicer_to_server(ctrlr, server)

        import socket
        listen_addr = f'{socket.gethostname()}:{port}'
        server.add_insecure_port(listen_addr)

        server.start()

        console.print(f'{ctrlr.name} was started on {listen_addr}')

        return server

    def controller_shutdown():
        console.print('Requested termination')
        ctrlr.terminate()

    def shutdown(sig, frame):
        console.print(f'Received {sig}')
        try:
            controller_shutdown()
        except:
            from drunc.utils.utils import print_traceback
            print_traceback()

        import os
        os.kill(os.getpid(), signal.SIGQUIT)


    terminate_signals = [signal.SIGHUP, signal.SIGPIPE]
    # terminate_signals = set(signal.Signals) - set([signal.SIGKILL, signal.SIGSTOP])
    for sig in terminate_signals:
        signal.signal(sig, shutdown)

    try:
        server = serve(control_port)
        server.wait_for_termination(timeout=None)

    except Exception as e:
        from drunc.utils.utils import print_traceback
        print_traceback()


