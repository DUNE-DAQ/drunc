from drunc.controller.interface.controller import controller_cli
from drunc.utils.utils import get_logger, get_root_logger


def main() -> None:
    try:
        controller_cli()
    except Exception as e:
        get_root_logger("INFO")
        log = get_logger("controller_app", rich_handler=True)
        log.error("Exception thrown!")
        log.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
