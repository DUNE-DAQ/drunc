from drunc.session_manager.session_manager_driver import SessionManagerDriver
from drunc.utils.shell_utils import ShellContext


class SessionManagerContext(ShellContext):
    shell_id = "session_manager_shell"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def create_drivers(self, **kwargs) -> dict[str, object]:
        if not self.address:
            return {}
        return {
            "session_manager": SessionManagerDriver(
                self.address,
                self._token,
            )
        }
