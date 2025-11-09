from drunc.session_manager.interface.session_manager import session_manager_cli
from drunc.utils.utils import create_root_logger, get_logger


def main():
    try:
        session_manager_cli()
    except Exception as e:
        create_root_logger("INFO")
        log = get_logger("session_manager", rich_handler=True)
        log.error("Exception thrown!")
        log.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
