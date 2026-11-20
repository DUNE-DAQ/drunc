from dataclasses import dataclass

from rich.console import Console

from drunc.run_control.run_control_driver import RunControlDriver

SHELL_ID = "run_control_shell"


@dataclass
class RunControlContext:
    run_control_driver: RunControlDriver
    console: Console = Console()
    override_logs: bool = False


# TODO: override_logs should be propagated to the shell initialisation.
