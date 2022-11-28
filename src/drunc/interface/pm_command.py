import click

@click.command()
@click.argument('port', type=int)
def pm_command(port:int, name=str):
    from drunc.process_manager.process_manager import ProcessManager

    pm = ProcessManager()

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





