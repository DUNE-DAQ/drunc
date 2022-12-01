import click
import asyncio
import grpc

@click.command()
@click.argument('port', type=int)
def pm_command(port:int, name=str):
    from drunc.process_manager.ssh_process_manager import SSHProcessManager
    from drunc.communication.process_manager_pb2_grpc import ProcessManagerServicer, add_ProcessManagerServicer_to_server
    
    pm = SSHProcessManager()

    async def serve(port:int) -> None:
        if not port:
            raise RuntimeError('The port on which to expect commands/send status wasn\'t specified')
        server = grpc.aio.server()
        add_ProcessManagerServicer_to_server(pm, server)
        # add_CommandProcessorServicer_to_server(ctrlr, server)
        # add_PingProcessorServicer_to_server   (ctrlr, server)
        listen_addr = f'[::]:{port}'
        server.add_insecure_port(listen_addr)
        await server.start()
        print(f'pm was started on {listen_addr}')
        await server.wait_for_termination()

    asyncio.run(serve(port))
