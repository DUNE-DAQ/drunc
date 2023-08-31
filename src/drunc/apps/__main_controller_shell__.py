
def main() -> None:
    from drunc.controller.interface.controller_shell import controller_shell, ControllerContext
    from rich.console import Console
    console = Console()
    context = ControllerContext()
    try:
        controller_shell(obj = context)
    except Exception:
        import os
        console.print_exception(width=os.get_terminal_size()[0])


if __name__ == '__main__':
    main()
