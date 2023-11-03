
def main() -> None:
    from drunc.controller.interface.context import ControllerContext
    context = ControllerContext()

    try:
        from drunc.controller.interface.shell import controller_shell
        controller_shell(obj = context)

    except Exception as e:
        if context.print_traceback:
            from drunc.utils.utils import print_traceback
            print_traceback()
        from rich import print as rprint
        rprint(f'[red bold]:fire::fire: Exception thrown :fire::fire:\n[/][yellow]{e}[/]\nUse [blue]--traceback[/] for more information\nExiting...')
        exit(1)





if __name__ == '__main__':
    main()
