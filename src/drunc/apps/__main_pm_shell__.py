
def main():
    from drunc.process_manager.interface.context import ProcessManagerContext
    context = ProcessManagerContext()

    try:
        from drunc.process_manager.interface.shell import process_manager_shell

        process_manager_shell(obj = context)

    except Exception as e:
        from rich import print as rprint
        rprint(f'[red bold]:fire::fire: Exception thrown :fire::fire:')
        from drunc.utils.utils import print_traceback
        print_traceback()
        rprint(f'Exiting...')
        exit(1)



if __name__ == '__main__':
    main()
