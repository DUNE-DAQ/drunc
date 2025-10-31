from drunc.process_manager.interface.process_manager import process_manager_cli
from drunc.utils.utils import create_root_logger, get_logger


def main():
    try:
        process_manager_cli()
    except Exception as e:
        create_root_logger("INFO", rich_handler=False)
        log = get_logger("process_manager")
        log.error("Exception thrown!")
        log.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
