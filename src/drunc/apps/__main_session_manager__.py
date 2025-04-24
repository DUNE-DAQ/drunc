from drunc_core.utils.utils import (
    create_logger_handler,
    get_logger,
    setup_root_logger,
)

from drunc.session_manager.interface.session_manager import session_manager_cli


def main():
    try:
        session_manager_cli()
    except Exception as e:
        setup_root_logger()
        log = get_logger("session_manager")
        create_logger_handler(rich_handler=False)
        log.error("Exception thrown!")
        log.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
