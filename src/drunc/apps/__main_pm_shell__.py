
def main():
    from drunc.process_manager.interface.process_manager_shell import process_manager_shell, PMContext
    from rich.console import Console
    console = Console()
    context = PMContext()
    try:
        process_manager_shell(obj = context)
    except Exception:
        import os
        console.print_exception(width=os.get_terminal_size()[0])


if __name__ == '__main__':
    main()
