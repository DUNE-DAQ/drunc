"""Rerun all jobs from the latest testing workflow run for a PR's current head."""

from __future__ import annotations

import html
import json
import os
import re
import sys
from pathlib import Path

from lib_github_api import GitHubApi, RoutingError, append_summary

COMMAND = "/rerun-integration-tests"
SHA_PATTERN = re.compile(r"^[0-9a-fA-F]{40,64}$")


def _safe_markdown(value: object) -> str:
    text = html.escape(str(value), quote=False).replace("\\", "\\\\")
    for character in "`*_{}[]()#+-.!|":
        text = text.replace(character, f"\\{character}")
    return text.replace("\r", " ").replace("\n", " ")


def _run_matches_pull(run: object, pull_number: int, head_sha: str) -> bool:
    if not isinstance(run, dict) or run.get("event") != "pull_request":
        return False
    linked_pulls = run.get("pull_requests") or []
    if not isinstance(linked_pulls, list):
        return False
    for linked_pull in linked_pulls:
        if not isinstance(linked_pull, dict) or linked_pull.get("number") != pull_number:
            continue
        linked_head = linked_pull.get("head")
        linked_sha = linked_head.get("sha") if isinstance(linked_head, dict) else None
        if isinstance(linked_sha, str) and linked_sha.casefold() == head_sha.casefold():
            return True
    return False


def rerun(api: GitHubApi, event: dict) -> str:
    """Rerun the latest completed full test run matching a human PR command."""
    issue = event.get("issue")
    comment = event.get("comment")
    if not isinstance(issue, dict) or not isinstance(comment, dict):
        return "Skipped: issue-comment event metadata is incomplete."
    body = comment.get("body")
    user = comment.get("user")
    if (
        event.get("action") != "created"
        or not isinstance(issue.get("pull_request"), dict)
        or not isinstance(user, dict)
        or user.get("type") != "User"
        or not isinstance(body, str)
        or COMMAND not in {line.strip() for line in body.splitlines()}
    ):
        return "Skipped: not a human PR comment containing the standalone rerun command."

    pull_number = issue.get("number")
    if isinstance(pull_number, bool) or not isinstance(pull_number, int) or pull_number <= 0:
        raise RoutingError("Comment event has no valid pull request number")
    pull = api.request("GET", f"pulls/{pull_number}")
    if not isinstance(pull, dict):
        raise RoutingError("GitHub did not return pull request metadata")
    if pull.get("state") != "open" or pull.get("draft"):
        return "Skipped: pull request is closed or draft."
    head = pull.get("head")
    head_sha = head.get("sha") if isinstance(head, dict) else None
    if not isinstance(head_sha, str) or not SHA_PATTERN.fullmatch(head_sha):
        raise RoutingError("Open pull request has no valid current head SHA")

    runs = api.paginated(
        "actions/workflows/testing.yml/runs",
        params={"event": "pull_request"},
        collection_key="workflow_runs",
    )
    matching_runs = [
        run
        for run in runs
        if _run_matches_pull(run, pull_number, head_sha)
    ]
    if not matching_runs:
        raise RoutingError(
            "No testing.yml run found for this PR's current head; a qualifying PR build must run first"
        )

    run = max(
        matching_runs,
        key=lambda candidate: candidate.get("run_number", 0)
        if isinstance(candidate.get("run_number"), int)
        else 0,
    )
    if run.get("status") != "completed":
        return "Skipped: the full testing workflow for the current PR revision is already active."
    run_id = run.get("id")
    if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
        raise RoutingError("Matching testing workflow run has no valid run ID")

    api.request("POST", f"actions/runs/{run_id}/rerun")
    return f"Full testing workflow rerun requested for run {run_id}."


def main() -> int:
    """Read the issue-comment event and request a full workflow rerun."""
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        raise RoutingError("GITHUB_EVENT_PATH is required")
    try:
        event = json.loads(Path(event_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RoutingError(f"Could not read GitHub event payload: {error}") from error
    if not isinstance(event, dict):
        raise RoutingError("GitHub event payload must be a JSON object")
    message = rerun(GitHubApi.from_environment(), event)
    print(message)
    append_summary(f"### Full integration test rerun\n\n{_safe_markdown(message)}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyError, ValueError, OSError, RoutingError) as error:
        try:
            append_summary(
                "### Full integration test rerun failed\n\n" + _safe_markdown(error)
            )
        except OSError:
            pass
        print("Full integration test rerun failed; see the Actions summary.", file=sys.stderr)
        sys.exit(1)