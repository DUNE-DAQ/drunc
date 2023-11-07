



def main():
    from drunc.process_manager.interface.session_manager import session_manager_cli
    try:
        session_manager_cli()
    except Exception:
        from drunc.utils.utils import print_traceback
        print_traceback()


if __name__ == '__main__':
    main()
