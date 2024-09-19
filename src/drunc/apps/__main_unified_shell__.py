
def main():
    from drunc.unified_shell.context import UnifiedShellContext
    context = UnifiedShellContext()

    try:
        from drunc.unified_shell.shell import unified_shell
        unified_shell(obj = context)

    except Exception as e:
        from rich import print as rprint
        rprint(f'[red bold]:fire::fire: Exception thrown :fire::fire:')
        from drunc.utils.utils import print_traceback
        print_traceback()
        rprint(f'Exiting...')

        if context.pm_process and context.pm_process.is_alive():
            context.pm_process.kill() # We're in an exception handler, so we are not going to do it half-heartedly, send a good ol' SIGKILL

        exit(1)


if __name__ == '__main__':
    main()
