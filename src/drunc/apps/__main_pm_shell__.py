
def main():
    from drunc.process_manager.interface.process_manager_shell import process_manager_shell, PMContext
    from rich.console import Console
    console = Console()
    context = PMContext()
    try:
        process_manager_shell(obj = context)
    except Exception as e:
        console.log("[bold red]Exception caught[/bold red]")
        if not context.print_traceback:
            console.log(e)
        else:
            console.print_exception()



if __name__ == '__main__':
    main()
