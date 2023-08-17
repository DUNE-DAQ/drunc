
def main() -> None:
    from drunc.controller.interface.controller_shell import controller_shell, ControllerContext
    from rich.console import Console
    console = Console()
    context = ControllerContext()
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
