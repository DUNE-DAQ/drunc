"""
Constants and helpers shared by the scripts that keep pull requests up to date on the
DRUNC Internal project board (track_new_pr_drunc_internal.py and sync_pr_draft_status.py).
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable
from pathlib import Path

from lib_github_api import RoutingError, append_summary

ORG = "DUNE-DAQ"

PROJECT_NUMBER = 8  # DRUNC Internal

STATUS_FIELD = "Status"

# Matches "In Progress", "In progress", "🏗 In progress", ...
IN_PROGRESS = re.compile(r"in progress$", re.IGNORECASE)

# Matches "Ready for review", "Ready for Review", "👀 Ready for review", ...
READY_FOR_REVIEW = re.compile(r"ready for review$", re.IGNORECASE)


def option_matching(field: dict, predicate: Callable[[str], bool]) -> dict:
    """
    Return the first single-select option whose name satisfies `predicate`.

    Args:
        field (dict): The project field containing single-select options.
        predicate (Callable[[str], bool]): A function to test option names.

    Returns:
        dict: The first option that satisfies the predicate.
    """
    for option in field.get("options") or []:
        if predicate(option["name"]):
            return option
    names = ", ".join(repr(option["name"]) for option in field.get("options") or [])
    raise RoutingError(f"No matching option in '{field['name']}' (options: {names})")


def pull_request_from_event() -> dict:
    """
    Read the pull request from the Actions event payload.

    Args:
        None

    Returns:
        dict: The pull request data from the event payload.
    """
    # Read the event payload from the file specified by GITHUB_EVENT_PATH.
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        raise RoutingError("GITHUB_EVENT_PATH is required")

    # Load the event payload from the file.
    event = json.loads(Path(event_path).read_text(encoding="utf-8"))

    # Extract the pull request from the event payload.
    pull = event.get("pull_request")
    if not isinstance(pull, dict) or not pull.get("node_id"):
        raise RoutingError("The event has no pull request node ID")

    return pull


def report(lines: list[str]) -> None:
    """
    Print the summary lines and append them to the workflow step summary.

    Args:
        lines (list[str]): The summary lines to report.

    Returns:
        None
    """
    # Join the summary lines and print them to the workflow log
    summary = "\n".join(lines)
    print(summary)

    # Append the summary to the workflow summary section
    append_summary(summary)
