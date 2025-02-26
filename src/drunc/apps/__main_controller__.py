from drunc.controller.interface.controller import controller_cli
from drunc.utils.utils import get_logger, setup_root_logger


def main():
    try:
        controller_cli()
    except Exception as e:
        setup_root_logger("ERROR")
        log = get_logger("controller", rich_handler=True)
        log.error("[red bold]:fire::fire: Exception thrown :fire::fire:")
        log.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
