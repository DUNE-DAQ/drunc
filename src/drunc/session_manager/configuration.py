"""Configuration for the session manager service."""

from drunc.utils.configuration import ConfHandler


class SessionManagerConfHandler(ConfHandler):
    """Handler for session manager configuration."""

    def populate_from_dict(self, data: dict[str, object]) -> None:
        pass
