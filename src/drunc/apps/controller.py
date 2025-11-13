from drunc.controller.interface.controller import controller_cli
from drunc.utils.utils import get_root_logger, get_logger


def main():
    try:
        controller_cli()
    except Exception as e:
        get_root_logger("INFO")
        log = get_logger("controller", rich_handler=False)
        log.error("Exception thrown!")
        log.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
