from drunc.utils.shell_utils import ShellContext


class RunControlContext(ShellContext):
    shell_id = "run_control"

    def __init__(self, *args, **kwargs) -> None:
        super(RunControlContext, self).__init__(*args, **kwargs)
