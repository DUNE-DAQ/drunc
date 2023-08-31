

def main():
    from drunc.controller.interface.controller import controller_cli
    try:
        controller_cli()
    except Exception:
        from rich.console import Console
        console = Console()
        import os
        console.print_exception(width=os.get_terminal_size()[0])

if __name__ == '__main__':
    main()

