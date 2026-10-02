from drunc.process_manager.interface.context import ProcessManagerContext
from drunc.process_manager.interface.shell import process_manager_shell
from drunc.utils.utils import get_logger, get_root_logger


def main() -> None:
    context = ProcessManagerContext()
    try:
        process_manager_shell(obj=context)
    except Exception as e:
        get_root_logger("INFO")
        log = get_logger("process_manager", rich_handler=True)
        log.error("[red bold]:fire::fire: Exception thrown :fire::fire:")
        log.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
