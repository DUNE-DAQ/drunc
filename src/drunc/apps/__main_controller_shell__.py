from drunc_core.utils.utils import (
    create_logger_handler,
    get_logger,
    setup_root_logger,
)

from drunc.controller.interface.context import ControllerContext
from drunc.controller.interface.shell import controller_shell


def main() -> None:
    context = ControllerContext()

    try:
        controller_shell(obj=context)

    except Exception as e:
        setup_root_logger()
        log = get_logger("controller_shell")
        create_logger_handler(rich_handler=True)
        log.error("[red bold]:fire::fire: Exception thrown :fire::fire:")
        log.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
