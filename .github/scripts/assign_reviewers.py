"""Pick reviewers for a pull request from its labels and update the review tracker."""

from __future__ import annotations

import os
import random
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import yaml
from lib_github_api import GitHubApi, GitHubApiError, RoutingError, append_summary
from lib_review_tracker import Checklist, ReviewTracker


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

    return RoutingConfig(labels=pools)


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
    history: dict[str, list[str]],
) -> tuple[dict[str, list[str]], list[str]]:
    """Request one eligible reviewer per label without replacing existing reviews."""
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

    return routes, notes


def main() -> int:
    """Run preview or apply routing based on the pull request event."""
    repo_root = Path(__file__).resolve().parents[2]
    config = load_config(
        Path(
            os.environ.get(
                "ROUTING_CONFIG", repo_root / ".github/mapping/reviewer-map.yml"
            )
        ),
        Path(
            os.environ.get(
                "LABELER_CONFIG", repo_root / ".github/mapping/label-map.yml"
            )
        ),
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

    if event_draft or pull.get("draft") or pull.get("state") != "open":
        append_summary(
            markdown_summary(
                "Reviewer routing preview (draft)",
                routes,
                ["Preview only; no review requests or comments were created."],
                ReviewTracker.render_reviewer_list(routes),
            )
        )
        return 0

    tracker = ReviewTracker.from_comments(
        api.paginated(f"issues/{pull_number}/comments"), Checklist.load()
    )
    # Ready PRs that missed ready_for_review (e.g. predating this workflow) still need a tracker.
    if event_action != "ready_for_review" and tracker.exists:
        append_summary(
            markdown_summary(
                "Reviewer routing",
                {},
                ["Review tracker already exists; nothing to do."],
            )
        )
        return 0

    applied_routes, notes = apply_routes(
        api,
        pull_number,
        current_labels,
        config,
        selector,
        pull_author,
        tracker.history,
    )
    tracker.save(api, pull_number, applied_routes)

    append_summary(markdown_summary("Reviewer routing", applied_routes, notes))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyError, ValueError, RoutingError) as error:
        print(f"Reviewer routing failed: {error}", file=sys.stderr)
        sys.exit(1)
