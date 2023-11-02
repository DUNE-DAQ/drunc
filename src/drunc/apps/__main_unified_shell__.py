
def main():
    from drunc.controller.interface.unified_shell import unified_shell, UnifiedShellContext
    context = UnifiedShellContext()

    try:
        unified_shell(obj = context)

    except Exception as e:
        if context.print_traceback:
            from drunc.utils.utils import print_traceback
            print_traceback()
        from rich import print as rprint
        rprint(f'[red bold]:fire::fire: Exception thrown :fire::fire:\n[/][yellow]{e}[/]\nUse [blue]--traceback[/] for more information\nExiting...')
        exit(1)


if __name__ == '__main__':
    main()
