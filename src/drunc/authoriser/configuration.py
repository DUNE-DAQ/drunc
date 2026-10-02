from drunc.utils.configuration import ConfHandler


class DummyAuthoriserConfHandler(ConfHandler):
    """Handler for dummy authoriser configuration."""

    def populate_from_dict(self, data: dict[str, object]) -> None:
        pass
