import click
import signal
from drunc.utils.utils import log_levels,  update_log_level, validate_command_facility

@click.command()
@click.argument('configuration', type=str)
@click.argument('command-facility', type=str, callback=validate_command_facility)#, help=f'Command facility (protocol, host and port) grpc://{socket.gethostname()}:12345')
@click.argument('name', type=str)
@click.argument('session', type=str)
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
def controller_cli(configuration:str, command_facility:str, name:str, session:str, log_level:str):

    from rich.console import Console
    console = Console()

    update_log_level(log_level)
    from logging import getLogger
    log = getLogger('controller_cli')
    from drunc.controller.controller import Controller
    from drunc.controller.configuration import ControllerConfHandler
    from druncschema.controller_pb2_grpc import add_ControllerServicer_to_server
    from druncschema.token_pb2 import Token
    token = Token(
        user_name = "controller_init_token",
        token = '',
    )

    from drunc.utils.configuration import parse_conf_url, OKSKey
    conf_path, conf_type = parse_conf_url(configuration)
    controller_configuration = ControllerConfHandler(
        type = conf_type,
        data = conf_path,
        oks_key = OKSKey(
            schema_file='schema/confmodel/dunedaq.schema.xml',
            class_name="RCApplication",
            obj_uid=name,
            session=session, # some of the function for enable/disable require the full dal of the session
        ),
    )

    ctrlr = Controller(
        name = name,
        session = session,
        configuration = controller_configuration,
        token = token,
    )

    def serve(listen_addr:str) -> None:
        import grpc
        from concurrent import futures
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))

        add_ControllerServicer_to_server(ctrlr, server)
        port = server.add_insecure_port(listen_addr)

        server.start()
        log.info(f'\'{ctrlr.name}\' was started on \'{port}\'')
        return server, port

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
        os.kill(os.getpid(), signal.SIGKILL)

    terminate_signals = [signal.SIGHUP] # Only SIGHUP - killing the tunnels
    for sig in terminate_signals:
        signal.signal(sig, shutdown)

    try:
        from drunc.utils.utils import resolve_localhost_and_127_ip_to_network_ip
        command_facility = resolve_localhost_and_127_ip_to_network_ip(command_facility)
        server_name = command_facility.split(':')[0]
        server, port = serve(command_facility)

        ctrlr.advertise_control_address(f'grpc://{server_name}:{port}')

        server.wait_for_termination(timeout=None)

    except Exception as e:
        from drunc.utils.utils import print_traceback
        print_traceback()


