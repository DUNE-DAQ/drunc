"""The bot's review tracker comment and the reviewer checklist it contains.

The tracker comment holds the hidden reviewer-assignment history, the visible
list of requested reviewers, and the reviewer checklist.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from lib_github_api import GitHubApi, bot_config

DEFAULT_CHECKLIST_PATH = (
    Path(__file__).resolve().parents[1] / "pr-bot-comments" / "reviewer-checklist.yml"
)

_TRACKER_MARKER = bot_config()["markers"]["tracker"]
_STATE_PREFIX = bot_config()["markers"]["tracker_state_prefix"]
_STATE_SUFFIX = " -->"
_CHECKLIST_START = bot_config()["markers"]["checklist_start"]
_CHECKLIST_END = bot_config()["markers"]["checklist_end"]
_ITEM_ID = re.compile(r"[a-z0-9][a-z0-9-]*")
_ITEM_LINE = re.compile(
    r"^- \[([ xX])\] (.+?) <!-- checklist:([a-z0-9][a-z0-9-]*) -->$"
)


class TrackerError(ValueError):
    """Raised when the tracker comment or the checklist config is invalid."""


@dataclass(frozen=True)
class ChecklistItem:
    """One required item in the reviewer checklist."""

    id: str
    text: str


@dataclass(frozen=True)
class Checklist:
    """The required items and footer from reviewer-checklist.yml."""

    items: tuple[ChecklistItem, ...]
    footer: str = ""

    @classmethod
    def load(cls, path: Path = DEFAULT_CHECKLIST_PATH) -> Checklist:
        """Load and validate the checklist config."""
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as error:
            raise TrackerError(
                f"Could not load reviewer checklist config: {error}"
            ) from error
        if not isinstance(raw, dict):
            raise TrackerError("Reviewer checklist config must be a mapping")
        return cls(
            items=cls._load_items(raw.get("items")),
            footer=cls._load_footer(raw.get("footer")),
        )

    @staticmethod
    def _load_items(raw_items: object) -> tuple[ChecklistItem, ...]:
        if not isinstance(raw_items, list) or not raw_items:
            raise TrackerError(
                "Reviewer checklist config must contain a non-empty items list"
            )
        items: list[ChecklistItem] = []
        seen: set[str] = set()
        for index, entry in enumerate(raw_items):
            item_id = entry.get("id") if isinstance(entry, dict) else None
            text = entry.get("text") if isinstance(entry, dict) else None
            if not isinstance(item_id, str) or not _ITEM_ID.fullmatch(item_id):
                raise TrackerError(
                    f"Checklist item {index} has an invalid id: {item_id!r}"
                )
            if not isinstance(text, str) or not text.strip() or "\n" in text:
                raise TrackerError(f"Checklist item {item_id} needs single-line text")
            if item_id in seen:
                raise TrackerError(f"Duplicate checklist item id in config: {item_id}")
            seen.add(item_id)
            items.append(ChecklistItem(item_id, text.strip()))
        return tuple(items)

    @staticmethod
    def _load_footer(footer: object) -> str:
        if footer is None:
            return ""
        if not isinstance(footer, str):
            raise TrackerError("Reviewer checklist footer must be a string")
        # The footer sits outside the checklist markers, so it must not forge them.
        if _CHECKLIST_START in footer or _CHECKLIST_END in footer:
            raise TrackerError(
                "Reviewer checklist footer must not contain checklist markers"
            )
        return footer.strip()

    def render(self, ticks: dict[str, bool]) -> str:
        """Render the checklist block; unknown IDs drop out, new items start unticked."""
        lines = [_CHECKLIST_START]
        lines.extend(
            f"- [{'x' if ticks.get(item.id, False) else ' '}] {item.text} "
            f"<!-- checklist:{item.id} -->"
            for item in self.items
        )
        lines.append(_CHECKLIST_END)
        if self.footer:
            lines.extend(["", self.footer])
        return "\n".join(lines)

    @staticmethod
    def parse(body: str) -> dict[str, bool]:
        """Read tick states by item ID; empty if the body has no checklist block."""
        if _CHECKLIST_START not in body and _CHECKLIST_END not in body:
            return {}
        if body.count(_CHECKLIST_START) != 1:
            raise TrackerError(
                "Reviewer checklist start marker is missing or duplicated"
            )
        if body.count(_CHECKLIST_END) != 1:
            raise TrackerError("Reviewer checklist end marker is missing or duplicated")
        start = body.index(_CHECKLIST_START) + len(_CHECKLIST_START)
        end = body.index(_CHECKLIST_END)
        if end < start:
            raise TrackerError("Reviewer checklist markers are out of order")

        ticks: dict[str, bool] = {}
        for line in body[start:end].strip("\n").splitlines():
            if not line.strip():
                continue
            match = _ITEM_LINE.fullmatch(line)
            if not match:
                raise TrackerError(f"Malformed reviewer checklist line: {line}")
            checked, _text, item_id = match.groups()
            if item_id in ticks:
                raise TrackerError(f"Duplicate reviewer checklist item ID: {item_id}")
            ticks[item_id] = checked.casefold() == "x"
        return ticks

    def missing(self, ticks: dict[str, bool]) -> list[ChecklistItem]:
        """Items configured but absent from the comment, e.g. added since it was posted."""
        return [item for item in self.items if item.id not in ticks]

    def unticked(self, ticks: dict[str, bool]) -> list[ChecklistItem]:
        """Items present in the comment but not ticked."""
        return [item for item in self.items if item.id in ticks and not ticks[item.id]]


@dataclass
class ReviewTracker:
    """The bot's tracker comment on one pull request."""

    checklist: Checklist
    comment: dict | None = None
    history: dict[str, list[str]] = field(default_factory=dict)
    ticks: dict[str, bool] = field(default_factory=dict)

    @classmethod
    def from_comments(cls, comments: list[dict], checklist: Checklist) -> ReviewTracker:
        """Find the bot's tracker comment among the PR's comments and parse it."""
        matches = [
            comment
            for comment in comments
            if isinstance(comment.get("body"), str)
            and _TRACKER_MARKER in comment["body"]
            and isinstance(comment.get("user"), dict)
            and comment["user"].get("login") == bot_config()["bot_login"]
        ]
        if len(matches) > 1:
            raise TrackerError("Multiple review tracker comments found")
        if not matches:
            return cls(checklist)
        body = matches[0]["body"]
        return cls(
            checklist,
            comment=matches[0],
            history=cls._parse_history(body),
            ticks=Checklist.parse(body),
        )

    @staticmethod
    def _parse_history(body: str) -> dict[str, list[str]]:
        """Read the hidden username-to-labels history; ignore it if unreadable."""
        state_line = next(
            (line for line in body.splitlines() if line.startswith(_STATE_PREFIX)), None
        )
        if not state_line or not state_line.endswith(_STATE_SUFFIX):
            return {}
        try:
            raw = json.loads(state_line[len(_STATE_PREFIX) : -len(_STATE_SUFFIX)])
        except json.JSONDecodeError:
            return {}
        if not isinstance(raw, dict):
            return {}
        return {
            username: labels
            for username, labels in raw.items()
            if isinstance(username, str)
            and isinstance(labels, list)
            and all(isinstance(label, str) for label in labels)
        }

    @property
    def exists(self) -> bool:
        """Whether the tracker comment has already been posted."""
        return self.comment is not None

    def render(self, routes: dict[str, list[str]]) -> str:
        """Render the full comment body, folding new routes into the history."""
        history = {username: list(labels) for username, labels in self.history.items()}
        for username, labels in routes.items():
            history[username] = list(dict.fromkeys(history.get(username, []) + labels))
        state_line = (
            _STATE_PREFIX
            + json.dumps(history, sort_keys=True, separators=(",", ":"))
            + _STATE_SUFFIX
        )
        return "\n".join(
            [
                _TRACKER_MARKER,
                state_line,
                self.render_reviewer_list(routes),
                "",
                self.checklist.render(self.ticks),
            ]
        )

    def save(
        self, api: GitHubApi, pull_number: int, routes: dict[str, list[str]]
    ) -> None:
        """Post the tracker if new, update it if changed, otherwise do nothing."""
        body = self.render(routes)
        if self.comment is None:
            api.request("POST", f"issues/{pull_number}/comments", {"body": body})
        elif self.comment["body"] != body:
            api.request(
                "PATCH", f"issues/comments/{self.comment['id']}", {"body": body}
            )

    @staticmethod
    def render_reviewer_list(routes: dict[str, list[str]]) -> str:
        """Render the human-visible list of requested reviewers."""
        if not routes:
            return "No reviewers were selected for this pull request."
        lines = ["Review requested for:"]
        lines.extend(
            f"- @{username}: {', '.join(f'`{label}`' for label in labels)}"
            for username, labels in routes.items()
        )
        return "\n".join(lines)
