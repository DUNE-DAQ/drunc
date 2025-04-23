from drunc.controller.interface.controller import controller_cli
from drunc_core.utils.utils import (
    create_logger_handler,
    get_logger,
    setup_root_logger,
)


def main():
    try:
        controller_cli()
    except Exception as e:
        setup_root_logger("INFO")
        log = get_logger("controller")
        create_logger_handler(rich_handler=False)
        log.error("Exception thrown!")
        log.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
