



def main():
    from drunc.process_manager.interface.process_manager import process_manager_cli
    try:
        process_manager_cli()
    except Exception:
        from rich.console import Console
        console = Console()
        import os
        console.print_exception(width=os.get_terminal_size()[0])


if __name__ == '__main__':
    main()
