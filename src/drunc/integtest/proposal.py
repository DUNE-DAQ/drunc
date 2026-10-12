from enum import Enum


class TableKind(Enum):
    pass


class LogLines:
    def __init__(self, lines):
        pass

    def require(
        self,
        *,
        text=None,
        pattern=None,
        echo=None,
        start_idx=0,
        index=False,
        error_message=None,
    ):
        pass

    def between(self, start, end, *, echo=True, start_idx=0):
        pass

    def assert_contains_between(
        self, start, end, expected_text, *, echo=False, start_idx=0
    ):
        pass

    def _strip_ansi(self, text):
        pass

    def _find(self, predicate, *, start_idx=0):
        pass

    def _validate_selector(self, *, text, pattern, echo):
        pass

    def _matches_marker(self, line, marker, *, echo=True):
        pass


class TableReader:
    def __init__(self, output):
        pass

    def after_echo(self, marker, kind, *, end_marker=None):
        pass

    def rows(self, table, kind, *, name):
        pass

    def value(self, table, kind, *, name, column):
        pass

    def assert_status(
        self,
        table,
        kind,
        *,
        state=None,
        controller_substate=None,
        non_controller_substate=None,
    ):
        pass

    def assert_uuids(self, table, kind):
        pass

    def assert_presence(self, table, kind, *, name, present=True, context=""):
        pass

    def assert_execution_success(self, table, kind):
        pass

    def _filter_rows(self, table, column, value):
        pass

    def _get_section(self, marker, *, end_marker=None):
        pass

    def _get_table_spec(self, kind):
        pass

    def _find_table(self, lines, spec):
        pass

    def _parse_table(self, lines, start_idx, spec):
        pass


def require_pattern_match(text, pattern, *, error_message):
    pass


def check_file_containing(lines, file):
    pass


def assert_match_contains_uuid(text, pattern, *, error_message):
    pass
