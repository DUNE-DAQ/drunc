
def main():
    from drunc.process_manager.interface.process_manager_shell import process_manager_shell, PMContext
    from rich.console import Console
    console = Console()
    context = PMContext()
    try:
        process_manager_shell(obj = context)
    except Exception:
        from drunc.utils.utils import print_traceback
        print_traceback()



if __name__ == '__main__':
    main()
