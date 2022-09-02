#!/usr/bin/env python3

"""
Command Line Interface for NanoRC
"""

def main():
    from rich.logging import RichHandler
    import logging
    from rich.console import Console
    from drunc.cli.cli import cli
    from drunc.cli.context import RCContext

    logging.basicConfig(
        level="INFO",
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)]
    )
    
    console = Console()
    context = RCContext(console)

    try:
        cli(obj=context, show_default=True)
    except Exception as e:
        console.log("[bold red]Exception caught[/bold red]")
        console.print_exception()

if __name__ == '__main__':
    main()
