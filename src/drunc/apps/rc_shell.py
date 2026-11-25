from drunc.run_control.interface.shell import rc_shell
from drunc.utils.utils import get_logger, get_root_logger


def main():
    try:
        rc_shell()
    except Exception as e:
        get_root_logger("INFO")
        log = get_logger("process_manager", rich_handler=True)
        log.error("[red bold]:fire::fire: Exception thrown :fire::fire:")
        log.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
