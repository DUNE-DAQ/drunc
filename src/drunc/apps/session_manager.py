from drunc.session_manager.interface.session_manager import session_manager_cli
from drunc.utils.utils import get_root_logger, get_logger


def main():
    try:
        session_manager_cli()
    except Exception as e:
        get_root_logger("INFO")
        log = get_logger("session_manager", rich_handler=False)
        log.error("Exception thrown!")
        log.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
