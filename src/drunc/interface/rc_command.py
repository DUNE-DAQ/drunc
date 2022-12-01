import click

from drunc.utils.utils import CONTEXT_SETTINGS, loglevels,  update_log_level

@click.command()
@click.argument('port', type=int)
@click.argument('name', type=str)
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')

def rc_command(port:int, name=str, log_level=str):
    
    update_log_level(log_level)
    
    from drunc.controller.controller import Controller
    from drunc.communication.command_pb2_grpc import CommandProcessorServicer, add_CommandProcessorServicer_to_server
    from drunc.communication.command_pb2_grpc import PingProcessorServicer, add_PingProcessorServicer_to_server
    import asyncio
    import grpc

    ctrlr = Controller(name)

    async def serve(port:int) -> None:
        if not port:
            raise RuntimeError('The port on which to expect commands/send status wasn\'t specified')
        server = grpc.aio.server()
        add_CommandProcessorServicer_to_server(ctrlr, server)
        add_PingProcessorServicer_to_server   (ctrlr, server)
        listen_addr = f'[::]:{port}'
        server.add_insecure_port(listen_addr)
        await server.start()
        print(f'{ctrlr.name} was started on {listen_addr}')
        await server.wait_for_termination()

    asyncio.run(serve(port))
