from drunc.controller.interface.context import ControllerContext
from drunc.controller.interface.shell import controller_shell
from drunc.utils.utils import get_logger, get_root_logger


def main() -> None:
    context = ControllerContext()

    try:
        controller_shell(obj=context)

    except Exception as e:
        get_root_logger("INFO")
        log = get_logger("controller_shell", rich_handler=True)
        log.error("[red bold]:fire::fire: Exception thrown :fire::fire:")
        log.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
