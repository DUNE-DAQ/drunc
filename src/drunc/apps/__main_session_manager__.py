from drunc.session_manager.interface.session_manager import session_manager_cli
from drunc.utils.utils import print_traceback


def main():
    try:
        session_manager_cli()
    except Exception:
        print_traceback()


if __name__ == "__main__":
    main()
