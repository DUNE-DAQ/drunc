"""Validate the authoritative reviewer checklist and publish a PR status."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from reviewer_checklist import (
    ChecklistError,
    authoritative_comments,
    parse_checklist_states,
    validate_required_items,
)
from reviewer_routing import GitHubApi, RoutingError

STATUS_CONTEXT = "reviewer-checklist"


def resolve_pull_numbers(event_name: str, event: dict, api: GitHubApi) -> list[int]:
    """Resolve pull request numbers from supported Actions event payloads."""
    if event_name == "pull_request":
        pull = event.get("pull_request", {})
        if not isinstance(pull, dict):
            raise RoutingError("Pull request event has invalid pull request metadata")
        number = pull.get("number")
        if isinstance(number, int):
            return [number]
        raise RoutingError("Pull request event has no pull request number")

    if event_name == "issue_comment":
        issue = event.get("issue", {})
        if not isinstance(issue, dict):
            raise RoutingError("Issue comment event has invalid issue metadata")
        if not issue.get("pull_request"):
            print(
                "Issue comment is not attached to a pull request; nothing to validate."
            )
            return []
        number = issue.get("number")
        if isinstance(number, int):
            return [number]
        raise RoutingError("Issue comment event has no pull request number")

    if event_name == "workflow_dispatch":
        inputs = event.get("inputs", {})
        if not isinstance(inputs, dict):
            raise RoutingError("workflow_dispatch inputs are malformed")
        value = inputs.get("pr_number") or os.environ.get("PR_NUMBER")
        try:
            number = int(value)
        except (TypeError, ValueError) as error:
            raise RoutingError(
                "workflow_dispatch requires a valid pr_number"
            ) from error
        if number < 1:
            raise RoutingError("workflow_dispatch requires a positive pr_number")
        return [number]

    if event_name == "workflow_run":
        run = event.get("workflow_run", {})
        if not isinstance(run, dict):
            raise RoutingError("workflow_run event has invalid run metadata")
        associated = run.get("pull_requests", [])
        if not isinstance(associated, list):
            associated = []
        pull_numbers = [
            pull.get("number")
            for pull in associated
            if isinstance(pull, dict) and isinstance(pull.get("number"), int)
        ]
        if not pull_numbers:
            run_id = run.get("id")
            if not isinstance(run_id, int):
                raise RoutingError("workflow_run event has no run ID")
            associated_pulls = api.request(
                "GET", f"actions/runs/{run_id}/pull_requests"
            )
            if isinstance(associated_pulls, list):
                pull_numbers = [
                    pull["number"]
                    for pull in associated_pulls
                    if isinstance(pull, dict) and isinstance(pull.get("number"), int)
                ]
        if pull_numbers:
            return sorted(set(pull_numbers))
        raise RoutingError(
            "Could not resolve a pull request for the completed workflow"
        )

    raise RoutingError(f"Unsupported GitHub Actions event: {event_name}")


def validate_pull_request(
    api: GitHubApi, pull_number: int
) -> tuple[str, str, str | None]:
    """Return the status state, description, and current head SHA for a pull request."""
    head_sha = None
    try:
        pull = api.request("GET", f"pulls/{pull_number}")
        if not isinstance(pull, dict):
            raise RoutingError("GitHub did not return pull request metadata")
        head = pull.get("head")
        head_sha = head.get("sha") if isinstance(head, dict) else None
        if not isinstance(head_sha, str) or not head_sha:
            raise RoutingError("Pull request metadata has no head SHA")
        if pull.get("state") == "closed" or pull.get("draft"):
            return "success", "Skipped: pull request is closed or draft", head_sha

        comments = api.paginated(f"issues/{pull_number}/comments")
        matches = authoritative_comments(comments)
        if not matches:
            return "failure", "Reviewer routing comment missing", head_sha
        if len(matches) != 1:
            return "failure", "Multiple reviewer routing comments found", head_sha

        checklist = parse_checklist_states(matches[0].get("body", ""))
        validate_required_items(checklist.states)
        return "success", "All required reviewer checklist items are complete", head_sha
    except ChecklistError as error:
        return "failure", str(error), head_sha
    except RoutingError as error:
        return "error", str(error), head_sha


def publish_status(api: GitHubApi, head_sha: str, state: str, description: str) -> None:
    """Publish the validation result against the PR's current head commit."""
    api.request(
        "POST",
        f"statuses/{head_sha}",
        {
            "state": state,
            "context": STATUS_CONTEXT,
            "description": description[:140],
        },
    )


def append_summary(pull_number: int, state: str, description: str) -> None:
    """Append one result to the GitHub Actions step summary when available."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as summary:
            summary.write(f"- PR #{pull_number}: **{state}** - {description}\n")


def should_publish_commit_status(event_name: str) -> bool:
    """Use the Actions job check for pull_request runs, not a duplicate status."""
    return os.environ.get("PUBLISH_STATUS", "true").casefold() == "true" and (
        event_name != "pull_request"
    )


def main() -> int:
    """Resolve event pull requests, validate fresh API data, and publish statuses."""
    token = os.environ.get("GITHUB_TOKEN")
    repository = os.environ.get("GITHUB_REPOSITORY")
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    event_name = os.environ.get("GITHUB_EVENT_NAME")
    if not token or not repository or not event_path or not event_name:
        raise RoutingError(
            "GITHUB_TOKEN, GITHUB_REPOSITORY, GITHUB_EVENT_PATH, and "
            "GITHUB_EVENT_NAME are required"
        )

    event = json.loads(Path(event_path).read_text(encoding="utf-8"))
    if not isinstance(event, dict):
        raise RoutingError("GitHub event payload must be a JSON object")
    api = GitHubApi(repository, token)
    pull_numbers = resolve_pull_numbers(event_name, event, api)
    publish_status_for_event = should_publish_commit_status(event_name)
    failed = False
    for pull_number in pull_numbers:
        state, description, head_sha = validate_pull_request(api, pull_number)
        print(f"PR #{pull_number}: {state}: {description}")
        append_summary(pull_number, state, description)
        if head_sha and publish_status_for_event:
            try:
                publish_status(api, head_sha, state, description)
            except RoutingError as error:
                print(
                    f"Could not publish {STATUS_CONTEXT} status: {error}",
                    file=sys.stderr,
                )
                failed = True
        elif not head_sha:
            failed = True
        failed = failed or state != "success"
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyError, ValueError, RoutingError) as error:
        print(f"Reviewer checklist validation failed: {error}", file=sys.stderr)
        sys.exit(1)
