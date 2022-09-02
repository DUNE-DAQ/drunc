import click

@click.command()
@click.argument('drunc-config')
@click.argument('daq-config')
@click.argument('partition')
@click.argument('user', default='user')
@click.pass_obj
def cli(obj, drunc_config, daq_config, partition, user):
    from drunc.core.configurations import SubControllerConfiguration
    from drunc.core.subcontroller import SubController
    console = obj.console

    from rich.table import Table
    from rich.panel import Panel
    grid = Table(title='drunc', expand=True, show_header=False, show_edge=False)
    grid.add_column()
    grid.add_row(f"Hello, {user}!")
    obj.console.print(grid)
    
    scc = SubControllerConfiguration.get_from_jsonfile(drunc_config)
    sc = SubController(scc, console)

    sc.run()
