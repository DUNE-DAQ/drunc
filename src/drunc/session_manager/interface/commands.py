import click


@click.command("describe")
@click.pass_context
def describe(ctx) -> None:
    """List the methods exposed by this endpoint."""
    response = ctx.obj.get_driver("session_manager").describe()
    click.echo(response)


@click.command("list_all_sessions")
@click.pass_context
def list_all_sessions(ctx) -> None:
    """List all active sessions."""
    response = ctx.obj.get_driver("session_manager").list_all_sessions()
    click.echo(response)


@click.command("list_all_configs")
@click.pass_context
def list_all_configs(ctx) -> None:
    """List all available configurations."""
    response = ctx.obj.get_driver("session_manager").list_all_configs()
    click.echo(response)


@click.command("load_session")
@click.argument("config_key", type=str)
@click.pass_context
def load_session(ctx, config_key: str) -> None:
    """Load a session based on the provided configuration key."""
    response = ctx.obj.get_driver("session_manager").load_session(config_key)
    click.echo(response)
