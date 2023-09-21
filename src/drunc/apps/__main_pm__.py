



def main():
    from drunc.process_manager.interface.process_manager import process_manager_cli
    try:
        process_manager_cli()
    except Exception:
        from drunc.utils.utils import print_traceback
        print_traceback()


if __name__ == '__main__':
    main()
