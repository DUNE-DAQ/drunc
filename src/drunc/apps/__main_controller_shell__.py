
def main() -> None:
    import nest_asyncio
    nest_asyncio.apply()
    from drunc.interface.controller_shell import controller_shell, RCContext
    from rich.console import Console
    console = Console()
    context = RCContext()
    try:
        controller_shell(obj = context)
    except Exception as e:
        console.log("[bold red]Exception caught[/bold red]")
        if not context.print_traceback:
            console.log(e)
        else:
            console.print_exception()

    
if __name__ == '__main__':
    main()
