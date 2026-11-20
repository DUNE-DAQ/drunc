"""Retry the current PR's checklist job when a human posts /checklist-complete."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from lib_github_api import GitHubApi, RoutingError, append_summary


def retry(api: GitHubApi, event: dict) -> str:
    """Request a checklist retry and return a message for the Actions summary."""
    issue = event.get("issue", {})
    comment = event.get("comment", {})
    if (
        event.get("action") != "created"
        or "pull_request" not in issue
        or comment.get("user", {}).get("type") != "User"
        or "/checklist-complete" not in comment.get("body", "")
    ):
        return "Skipped: not a human PR comment containing /checklist-complete."

    pull_number = issue.get("number")
    if (
        isinstance(pull_number, bool)
        or not isinstance(pull_number, int)
        or pull_number <= 0
    ):
        raise RoutingError("Comment event has no valid pull request number")
    pull = api.request("GET", f"pulls/{pull_number}")
    if not isinstance(pull, dict):
        raise RoutingError("GitHub did not return pull request metadata")
    if pull.get("state") != "open" or pull.get("draft"):
        return "Skipped: pull request is closed or draft."

    head_sha = pull["head"]["sha"]
    runs = api.paginated(
        "actions/workflows/pr_review.yml/runs",
        params={"event": "pull_request", "head_sha": head_sha},
        collection_key="workflow_runs",
    )
    matching_runs = [
        run
        for run in runs
        if any(
            linked_pull.get("number") == pull_number
            and linked_pull.get("head", {}).get("sha") == head_sha
            for linked_pull in run.get("pull_requests", [])
        )
    ]
    if not matching_runs:
        raise RoutingError(
            "No PR review run found for this PR and its current revision"
        )
    run = max(matching_runs, key=lambda candidate: candidate["run_number"])
    if run.get("status") != "completed":
        return "Skipped: the PR review workflow is already active."

    jobs = api.paginated(
        f"actions/runs/{run['id']}/jobs",
        params={"filter": "latest"},
        collection_key="jobs",
    )
    job = next(
        (
            candidate
            for candidate in jobs
            if candidate.get("name") == "reviewer-checklist"
        ),
        None,
    )
    if (
        not job
        or job.get("status") != "completed"
        or job.get("conclusion") == "skipped"
    ):
        raise RoutingError("No completed reviewer-checklist job is available to retry")

    api.request("POST", f"actions/jobs/{job['id']}/rerun")
    return f"Reviewer checklist retry requested: {run['html_url']}"


def main() -> int:
    """Read the issue_comment payload and request a retry using the shared client."""
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        raise RoutingError("GITHUB_EVENT_PATH is required")
    event = json.loads(Path(event_path).read_text(encoding="utf-8"))
    if not isinstance(event, dict):
        raise RoutingError("GitHub event payload must be a JSON object")
    message = retry(GitHubApi.from_environment(), event)
    print(message)
    append_summary(f"### Checklist retry\n- {message}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyError, ValueError, OSError, RoutingError) as error:
        print(f"Reviewer checklist retry failed: {error}", file=sys.stderr)
        sys.exit(1)
