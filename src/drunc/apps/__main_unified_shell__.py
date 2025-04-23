from drunc.unified_shell.context import UnifiedShellContext
from drunc.unified_shell.shell import unified_shell
from drunc_core.utils.utils import (
    create_logger_handler,
    get_logger,
    setup_root_logger,
)


def main():
    context = UnifiedShellContext()

    try:
        unified_shell(obj=context)

    except Exception as e:
        setup_root_logger("INFO")
        log = get_logger("unified_shell")
        create_logger_handler(rich_handler=True)
        log.error("[red bold]:fire::fire: Exception thrown :fire::fire:")
        log.exception(e)
        if context.pm_process and context.pm_process.is_alive():
            context.pm_process.kill()  # We're in an exception handler, so we are not going to do it half-heartedly, send a good ol' SIGKILL
        exit(1)


if __name__ == "__main__":
    main()
