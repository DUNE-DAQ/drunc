from drunc.run_control.interface.context import RunControlContext
from drunc.run_control.interface.shell import run_control_shell
from drunc.utils.utils import get_logger, get_root_logger


def main():
    context = RunControlContext()
    try:
        run_control_shell(obj=context)
    except Exception as e:
        get_root_logger("INFO")
        log = get_logger("run_control", rich_handler=True)
        log.error("[red bold]:fire::fire: Exception thrown :fire::fire:")
        log.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
