"""Fail unless every reviewer checklist item in the review tracker is ticked."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from lib_github_api import GitHubApi, RoutingError, append_summary
from lib_review_tracker import Checklist, ReviewTracker, TrackerError


def resolve_pull_number(event: dict) -> int:
    """Return the pull request number from a pull_request event payload."""
    pull = event.get("pull_request")
    number = pull.get("number") if isinstance(pull, dict) else None
    if not isinstance(number, int):
        raise RoutingError("Pull request event has no pull request number")
    return number


def verify(api: GitHubApi, pull_number: int) -> tuple[str, str]:
    """Return the result state and description for a pull request."""
    try:
        pull = api.request("GET", f"pulls/{pull_number}")
        if not isinstance(pull, dict):
            raise RoutingError("GitHub did not return pull request metadata")
        if pull.get("state") == "closed" or pull.get("draft"):
            return "success", "Skipped: pull request is closed or draft"

        tracker = ReviewTracker.from_comments(
            api.paginated(f"issues/{pull_number}/comments"), Checklist.load()
        )
        if not tracker.exists:
            return "failure", "Review tracker comment missing"
        if missing := tracker.checklist.missing(tracker.ticks):
            ids = ", ".join(item.id for item in missing)
            return "failure", f"Required checklist item(s) missing from tracker: {ids}"
        if unticked := tracker.checklist.unticked(tracker.ticks):
            total = len(tracker.checklist.items)
            done = total - len(unticked)
            return "failure", f"Reviewer checklist incomplete ({done}/{total} complete)"
        return "success", "All required reviewer checklist items are complete"
    except TrackerError as error:
        return "failure", str(error)
    except RoutingError as error:
        return "error", str(error)


def main() -> int:
    """Validate the checklist for the event's pull request using fresh API data."""
    token = os.environ.get("GITHUB_TOKEN")
    repository = os.environ.get("GITHUB_REPOSITORY")
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not token or not repository or not event_path:
        raise RoutingError(
            "GITHUB_TOKEN, GITHUB_REPOSITORY, and GITHUB_EVENT_PATH are required"
        )

    event = json.loads(Path(event_path).read_text(encoding="utf-8"))
    if not isinstance(event, dict):
        raise RoutingError("GitHub event payload must be a JSON object")
    api = GitHubApi(repository, token)
    pull_number = resolve_pull_number(event)
    state, description = verify(api, pull_number)
    print(f"PR #{pull_number}: {state}: {description}")
    append_summary(f"- PR #{pull_number}: **{state}** - {description}")
    return 0 if state == "success" else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyError, ValueError, RoutingError) as error:
        print(f"Reviewer checklist validation failed: {error}", file=sys.stderr)
        sys.exit(1)
