#!/usr/bin/env python3

"""
Command Line Interface for NanoRC
"""

def main():
    from rich.logging import RichHandler
    import logging
    from rich.console import Console

    logging.basicConfig(
        level="INFO",
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)]
    )
    
    console = Console()
    obj = RCContext(console)

    try:
        cli(obj=obj, show_default=True)
    except Exception as e:
        console.log("[bold red]Exception caught[/bold red]")
        if not obj.print_traceback:
            console.log(e)
        else:
            console.print_exception()

if __name__ == '__main__':
    main()
