from drunc.run_control.interface.context import RunControlContext
from drunc.run_control.interface.run_control import rc_cli
from drunc.utils.utils import get_logger, get_root_logger


def main():
    context = RunControlContext()
    try:
        rc_cli(obj=context)
    except Exception as e:
        get_root_logger("INFO")
        log = get_logger("run_control", rich_handler=True)
        log.error("Exception thrown!")
        log.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
