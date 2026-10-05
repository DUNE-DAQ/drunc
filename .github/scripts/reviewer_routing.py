"""Plan and apply label-based pull request reviewer routing."""

from __future__ import annotations

import json
import os
import random
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import yaml
from reviewer_checklist import (
    CHECKLIST_END_MARKER,
    CHECKLIST_START_MARKER,
    ROUTING_MARKER,
    ROUTING_STATE_PREFIX,
    ChecklistError,
    authoritative_comments,
    merge_with_required_items,
    parse_checklist_states,
    render_checklist,
)


class RoutingError(Exception):
    """An expected configuration or GitHub API error."""


class GitHubApiError(RoutingError):
    """An HTTP error returned by the GitHub API."""

    def __init__(self, status: int, detail: str):
        """Store the response status and body for policy decisions."""
        super().__init__(f"GitHub API returned {status}: {detail}")
        self.status = status
        self.detail = detail


@dataclass(frozen=True)
class Reviewer:
    username: str
    weight: int


class ReviewerSelector(Protocol):
    """Strategy interface for choosing one reviewer from a label pool."""

    def select(
        self,
        reviewers: list[Reviewer],
        context: SelectionContext,
    ) -> Reviewer | None:
        """Select one eligible reviewer or return None when the pool is empty."""
        ...


class GitHubReader(Protocol):
    """Read-only API available to selection strategies."""

    def get(self, path: str) -> dict | list:
        """Read a GitHub API resource without mutating repository state."""
        ...


@dataclass(frozen=True)
class SelectionContext:
    """Inputs and read-only repository access for a selector strategy."""

    pull_number: int
    label: str
    excluded: set[str]
    github: GitHubReader | None


class DeterministicSelector:
    """Choose one reviewer deterministically from a label's weighted pool.

    The selector first removes usernames listed in ``context.excluded``. It
    then seeds a pseudo-random number generator with the pull request number
    and label, draws a uniform integer in ``[0, total weight)``, and maps it onto the
    remaining reviewers' weights in configuration order.

    For example, with ``alice`` at weight 3 and ``bob`` at weight 1, ``alice``
    is selected with probability 3/4 and ``bob`` with probability 1/4.
    Re-running selection for the same PR, label, and unchanged pool returns
    the same candidate, while different labels on one PR draw independently.

    Exclusions are applied before drawing. If ``alice`` is the PR author, only
    ``bob`` remains and is always selected. The caller handles reviewer
    requests and can retry selection after excluding a candidate GitHub
    rejects.

    Weights are selection probabilities, not workload balancing or a rotation
    based on past reviews. This class selects a candidate; the routing code
    separately requests the review from GitHub.
    """

    def select(
        self,
        reviewers: list[Reviewer],
        context: SelectionContext,
    ) -> Reviewer | None:
        """Select by a weighted draw seeded with the PR and label, skipping exclusions."""
        # Remove the author (and any other exclusions) before summing weights so
        # excluded reviewers have zero probability of selection.
        eligible = [
            reviewer
            for reviewer in reviewers
            if reviewer.username.casefold() not in context.excluded
        ]
        total_weight = sum(reviewer.weight for reviewer in eligible)
        if not total_weight:
            return None

        # Per-label seed keeps reruns reproducible while decorrelating labels on one PR.
        seed = f"{context.pull_number}:{context.label}"
        draw = random.Random(seed).randrange(total_weight)
        for reviewer in eligible:
            if draw < reviewer.weight:
                return reviewer
            draw -= reviewer.weight
        raise AssertionError("Weighted selection did not resolve")


@dataclass(frozen=True)
class RoutingConfig:
    dry_run: bool
    labels: dict[str, list[Reviewer]]


def load_config(config_path: Path, labeler_path: Path) -> RoutingConfig:
    """Load and validate reviewer pools against the authoritative labeler."""
    try:
        raw_config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        raw_labeler = yaml.safe_load(labeler_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise RoutingError(f"Could not load routing configuration: {error}") from error

    if not isinstance(raw_config, dict) or not isinstance(raw_labeler, dict):
        raise RoutingError("Routing and labeler configuration must be mappings")
    raw_labels = raw_config.get("labels")
    if not isinstance(raw_labels, dict):
        raise RoutingError("Routing configuration must contain a labels mapping")
    if not isinstance(raw_config.get("dry_run"), bool):
        raise RoutingError("dry_run must be true or false")

    unknown = set(raw_labels) - set(raw_labeler)
    missing = set(raw_labeler) - set(raw_labels)
    if unknown or missing:
        details = []
        if unknown:
            details.append(f"unknown labels: {', '.join(sorted(unknown))}")
        if missing:
            details.append(f"unmapped labels: {', '.join(sorted(missing))}")
        raise RoutingError(
            "Reviewer label mapping mismatch (" + "; ".join(details) + ")"
        )

    pools: dict[str, list[Reviewer]] = {}
    for label, entry in raw_labels.items():
        if not isinstance(entry, dict) or not isinstance(entry.get("reviewers"), list):
            raise RoutingError(f"Label {label!r} must define a reviewers list")
        pool: list[Reviewer] = []
        usernames: set[str] = set()
        for candidate in entry["reviewers"]:
            if not isinstance(candidate, dict):
                raise RoutingError(f"Reviewer entries for {label!r} must be mappings")
            username = candidate.get("username")
            weight = candidate.get("weight")
            if not isinstance(username, str) or not re.fullmatch(
                r"[A-Za-z0-9_-]+", username
            ):
                raise RoutingError(f"Invalid reviewer username in label {label!r}")
            if isinstance(weight, bool) or not isinstance(weight, int) or weight <= 0:
                raise RoutingError(
                    f"Reviewer weights for {label!r} must be positive integers"
                )
            folded_username = username.casefold()
            if folded_username in usernames:
                raise RoutingError(
                    f"Duplicate reviewer {username!r} in label {label!r}"
                )
            usernames.add(folded_username)
            pool.append(Reviewer(username=username, weight=weight))
        if not pool:
            raise RoutingError(f"Reviewer pool for {label!r} cannot be empty")
        pools[label] = pool

    return RoutingConfig(dry_run=raw_config["dry_run"], labels=pools)


def plan_routes(
    labels: set[str],
    config: RoutingConfig,
    selector: ReviewerSelector,
    pull_number: int,
    author: str,
    github: GitHubReader | None = None,
) -> dict[str, list[str]]:
    """Return a reviewer-to-label mapping, excluding the pull request author."""
    planned: dict[str, list[str]] = {}
    for label, pool in config.labels.items():
        if label not in labels:
            continue
        reviewer = selector.select(
            pool,
            SelectionContext(pull_number, label, {author.casefold()}, github),
        )
        if reviewer is not None:
            planned.setdefault(reviewer.username, []).append(label)
    return planned


class GitHubApi:
    """Small GitHub REST client for routing metadata and reviewer requests."""

    def __init__(self, repository: str, token: str):
        """Initialise the client for one repository and workflow token."""
        owner, separator, name = repository.partition("/")
        if not separator or not owner or not name or "/" in name:
            raise RoutingError("GITHUB_REPOSITORY must be owner/name")
        self.owner = quote(owner, safe="")
        self.name = quote(name, safe="")
        self.token = token

    def request(self, method: str, path: str, body: dict | None = None) -> dict | list:
        """Send one authenticated request to the GitHub REST API."""
        url = f"https://api.github.com/repos/{self.owner}/{self.name}/{path}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = Request(
            url,
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "Content-Type": "application/json",
            },
        )
        try:
            with urlopen(request, timeout=20) as response:
                content = response.read()
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise GitHubApiError(error.code, detail) from error
        except (URLError, TimeoutError) as error:
            raise RoutingError(f"GitHub API request failed: {error}") from error
        return json.loads(content) if content else {}

    def paginated(self, path: str) -> list[dict]:
        """Fetch all pages for an API collection endpoint."""
        results: list[dict] = []
        page = 1
        while True:
            response = self.request("GET", f"{path}?per_page=100&page={page}")
            if not isinstance(response, list):
                raise RoutingError(f"Expected a list from GitHub endpoint {path}")
            results.extend(response)
            if len(response) < 100:
                return results
            page += 1

    def get(self, path: str) -> dict | list:
        """Expose a read-only request for selector strategies."""
        return self.request("GET", path)


def append_summary(text: str) -> None:
    """Append a plain-text section to the GitHub Actions step summary."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as summary:
            summary.write(text.rstrip() + "\n")


def visible_notification_body(routes: dict[str, list[str]]) -> str:
    """Render the human-visible PR comment without routing metadata."""
    if not routes:
        return "No reviewers were selected for this pull request."
    lines = ["Review requested for:"]
    lines.extend(
        f"- @{username}: {', '.join(f'`{label}`' for label in areas)}"
        for username, areas in routes.items()
    )
    return "\n".join(lines)


def markdown_summary(
    title: str,
    routes: dict[str, list[str]],
    notes: list[str],
    pr_comment_preview: str | None = None,
) -> str:
    """Render routes without triggering GitHub mentions."""
    lines = [f"### {title}"]
    if routes:
        lines.extend(
            f"- `{user}`: {', '.join(f'`{label}`' for label in areas)}"
            for user, areas in routes.items()
        )
    else:
        lines.append("- No reviewer routes selected.")
    lines.extend(f"- Note: {note}" for note in notes)
    if pr_comment_preview is not None:
        lines.extend(
            [
                "",
                "#### Proposed PR comment",
                "",
                "```text",
                pr_comment_preview,
                "```",
                "",
                "Preview only; the fenced text will not notify or tag reviewers.",
            ]
        )
    return "\n".join(lines)


def apply_routes(
    api: GitHubApi,
    pull_number: int,
    labels: set[str],
    config: RoutingConfig,
    selector: ReviewerSelector,
    author: str,
) -> tuple[dict[str, list[str]], list[str], dict[str, list[str]]]:
    """Request one eligible reviewer per label without replacing existing reviews."""
    comments = api.paginated(f"issues/{pull_number}/comments")
    bot_comments = authoritative_comments(comments)
    if len(bot_comments) > 1:
        raise RoutingError("Multiple authoritative reviewer routing comments found")
    bot_comment = bot_comments[0] if bot_comments else None
    state_prefix = ROUTING_STATE_PREFIX
    history: dict[str, list[str]] = {}
    if bot_comment:
        state_line = next(
            (
                line
                for line in bot_comment.get("body", "").splitlines()
                if line.startswith(state_prefix)
            ),
            None,
        )
        if state_line and state_line.endswith(" -->"):
            try:
                raw_history = json.loads(state_line[len(state_prefix) : -4])
            except json.JSONDecodeError:
                raw_history = {}
            if isinstance(raw_history, dict):
                history = {
                    username: area_list
                    for username, area_list in raw_history.items()
                    if isinstance(username, str)
                    and isinstance(area_list, list)
                    and all(isinstance(area, str) for area in area_list)
                }

    requested_response = api.request("GET", f"pulls/{pull_number}/requested_reviewers")
    review_response = api.paginated(f"pulls/{pull_number}/reviews")
    requested = {
        user["login"].casefold()
        for user in requested_response.get("users", [])
        if isinstance(user.get("login"), str)
    }
    reviewed = {
        review["user"]["login"].casefold()
        for review in review_response
        if isinstance(review.get("user", {}).get("login"), str)
    }
    routes: dict[str, list[str]] = {}
    notes: list[str] = []
    invalid_reviewers: set[str] = set()
    previous_assignee_by_label = {
        label: username for username, areas in history.items() for label in areas
    }

    for label, pool in config.labels.items():
        if label not in labels:
            continue
        previous_assignee = previous_assignee_by_label.get(label)
        if previous_assignee:
            folded_assignee = previous_assignee.casefold()
            if folded_assignee in requested:
                routes.setdefault(previous_assignee, []).append(label)
            elif folded_assignee in reviewed:
                notes.append(
                    f"{previous_assignee} has already reviewed for {label}; no replacement selected."
                )
            else:
                notes.append(
                    f"Preserving the previous {label} assignment; no replacement selected."
                )
            continue

        excluded = {author.casefold(), *invalid_reviewers}
        while reviewer := selector.select(
            pool,
            SelectionContext(pull_number, label, excluded, api),
        ):
            username = reviewer.username
            folded_username = username.casefold()
            if folded_username in reviewed:
                notes.append(
                    f"{username} has already reviewed for {label}; no replacement selected."
                )
                break
            if folded_username in requested:
                routes.setdefault(username, []).append(label)
                break
            try:
                api.request(
                    "POST",
                    f"pulls/{pull_number}/requested_reviewers",
                    {"reviewers": [username]},
                )
            except GitHubApiError as error:
                detail = error.detail.casefold()
                if error.status == 422 and "collaborator" in detail:
                    notes.append(
                        f"{username} cannot be requested for {label}; trying the next eligible reviewer."
                    )
                    excluded.add(folded_username)
                    invalid_reviewers.add(folded_username)
                    continue
                raise
            requested.add(folded_username)
            routes.setdefault(username, []).append(label)
            break
        else:
            notes.append(f"No eligible reviewer remains for {label}.")

    return routes, notes, history


def upsert_notification(
    api: GitHubApi,
    pull_number: int,
    routes: dict[str, list[str]],
    history: dict[str, list[str]],
) -> None:
    """Create or update the single routing comment owned by GitHub Actions."""
    comments = api.paginated(f"issues/{pull_number}/comments")
    bot_comments = authoritative_comments(comments)
    if len(bot_comments) > 1:
        raise RoutingError("Multiple authoritative reviewer routing comments found")
    bot_comment = bot_comments[0] if bot_comments else None
    checklist_states: dict[str, bool] = {}
    if bot_comment:
        existing_body = bot_comment.get("body", "")
        if (
            CHECKLIST_START_MARKER in existing_body
            or CHECKLIST_END_MARKER in existing_body
        ):
            try:
                checklist_states = parse_checklist_states(existing_body).states
            except ChecklistError as error:
                raise RoutingError(
                    f"Existing reviewer checklist is malformed: {error}"
                ) from error

    persisted_history = {username: list(areas) for username, areas in history.items()}
    for username, areas in routes.items():
        persisted_history.setdefault(username, [])
        persisted_history[username] = list(
            dict.fromkeys(persisted_history[username] + areas)
        )
    state = (
        ROUTING_STATE_PREFIX
        + json.dumps(persisted_history, sort_keys=True, separators=(",", ":"))
        + " -->"
    )
    body = "\n".join(
        [
            ROUTING_MARKER,
            state,
            visible_notification_body(routes),
            "",
            render_checklist(merge_with_required_items(checklist_states)),
        ]
    )
    if bot_comment:
        if bot_comment.get("body") != body:
            api.request("PATCH", f"issues/comments/{bot_comment['id']}", {"body": body})
        return
    api.request("POST", f"issues/{pull_number}/comments", {"body": body})


def main() -> int:
    """Run preview or apply routing based on the pull request event."""
    repo_root = Path(__file__).resolve().parents[2]
    config = load_config(
        Path(
            os.environ.get("ROUTING_CONFIG", repo_root / ".github/reviewer-routing.yml")
        ),
        Path(os.environ.get("LABELER_CONFIG", repo_root / ".github/labeler.yml")),
    )
    token = os.environ.get("GITHUB_TOKEN")
    repository = os.environ.get("GITHUB_REPOSITORY")
    pull_number = int(os.environ["PR_NUMBER"])
    author = os.environ["PR_AUTHOR"]
    event_action = os.environ["PR_ACTION"]
    event_draft = os.environ["PR_DRAFT"].lower() == "true"
    if not token or not repository:
        raise RoutingError("GITHUB_TOKEN and GITHUB_REPOSITORY are required")

    api = GitHubApi(repository, token)
    pull = api.request("GET", f"pulls/{pull_number}")
    if not isinstance(pull, dict):
        raise RoutingError("GitHub did not return pull request metadata")
    current_labels = {label["name"] for label in pull.get("labels", [])}
    pull_author = pull.get("user", {}).get("login", author)
    selector = (
        DeterministicSelector()
    )  # Swap this strategy to change reviewer selection.
    routes = plan_routes(
        current_labels, config, selector, pull_number, pull_author, api
    )

    if event_action != "ready_for_review":
        if event_draft or pull.get("draft"):
            append_summary(
                markdown_summary(
                    "Reviewer routing preview (draft)",
                    routes,
                    ["Preview only; no review requests or comments were created."],
                    visible_notification_body(routes),
                )
            )
        else:
            append_summary(
                markdown_summary(
                    "Reviewer routing",
                    {},
                    ["No assignment: this event was not ready_for_review."],
                )
            )
        return 0

    if event_draft or pull.get("draft") or pull.get("state") != "open":
        append_summary(
            markdown_summary(
                "Reviewer routing preview (draft)",
                routes,
                [
                    "The pull request is no longer open and ready; no review requests or comments were created."
                ],
                visible_notification_body(routes),
            )
        )
        return 0

    if config.dry_run:
        append_summary(
            markdown_summary(
                "Reviewer routing preview (global dry-run)",
                routes,
                [
                    "Global dry-run is enabled; no review requests or comments were created."
                ],
                visible_notification_body(routes),
            )
        )
        return 0

    applied_routes, notes, history = apply_routes(
        api, pull_number, current_labels, config, selector, pull_author
    )
    upsert_notification(api, pull_number, applied_routes, history)
    append_summary(markdown_summary("Reviewer routing", applied_routes, notes))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyError, ValueError, RoutingError) as error:
        print(f"Reviewer routing failed: {error}", file=sys.stderr)
        sys.exit(1)
