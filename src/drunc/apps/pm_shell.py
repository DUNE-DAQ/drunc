from drunc.process_manager.interface.context import ProcessManagerContext
from drunc.process_manager.interface.shell import process_manager_shell
from drunc.utils.utils import create_logger_handler, get_logger, setup_root_logger


def main():
    context = ProcessManagerContext()
    try:
        process_manager_shell(obj=context)
    except Exception as e:
        setup_root_logger("INFO")
        log = get_logger("process_manager")
        create_logger_handler(rich_handler=True)
        log.error("[red bold]:fire::fire: Exception thrown :fire::fire:")
        log.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
