"""Render and validate the checklist embedded in the reviewer routing comment."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Sequence

ROUTING_MARKER = "<!-- reviewer-routing:v1 -->"
ROUTING_STATE_PREFIX = "<!-- reviewer-routing-state:"
CHECKLIST_START_MARKER = "<!-- reviewer-checklist:start:v1 -->"
CHECKLIST_END_MARKER = "<!-- reviewer-checklist:end:v1 -->"
AUTHORITATIVE_BOT_LOGIN = "github-actions[bot]"


class ChecklistError(ValueError):
    """Raised when the routing comment contains an invalid checklist."""


@dataclass(frozen=True)
class ChecklistItem:
    """One required item in the reviewer checklist."""

    item_id: str
    text: str


@dataclass(frozen=True)
class ParsedChecklist:
    """Checklist item states keyed by their stable hidden IDs."""

    states: dict[str, bool]


def authoritative_comments(comments: list[dict]) -> list[dict]:
    """Return routing comments authored by the GitHub Actions bot."""
    return [
        comment
        for comment in comments
        if isinstance(comment.get("body"), str)
        and ROUTING_MARKER in comment["body"]
        and isinstance(comment.get("user"), dict)
        and comment["user"].get("login") == AUTHORITATIVE_BOT_LOGIN
    ]


_REQUIRED_ITEMS = (
    ChecklistItem("implementation", "Implementation reviewed."),
    ChecklistItem("tests-docs", "Tests and documentation reviewed."),
)
_ITEM_LINE = re.compile(
    r"^- \[([ xX])\] (.+?) <!-- checklist:([a-z0-9][a-z0-9-]*) -->$"
)


def required_items() -> list[ChecklistItem]:
    """Return the required reviewer checklist items in display order."""
    return list(_REQUIRED_ITEMS)


def merge_with_required_items(
    existing_states: dict[str, bool],
    items: Sequence[ChecklistItem] | None = None,
) -> dict[str, bool]:
    """Preserve known states and default newly required checklist items to unchecked."""
    return {
        item.item_id: existing_states.get(item.item_id, False)
        for item in (items if items is not None else required_items())
    }


def render_checklist(states: dict[str, bool]) -> str:
    """Render the marked checklist section using the current required items."""
    lines = [CHECKLIST_START_MARKER]
    lines.extend(
        f"- [{'x' if states.get(item.item_id, False) else ' '}] {item.text} "
        f"<!-- checklist:{item.item_id} -->"
        for item in required_items()
    )
    lines.append(CHECKLIST_END_MARKER)
    return "\n".join(lines)


def extract_checklist_section(body: str) -> str:
    """Return the checklist content, rejecting absent, repeated, or reversed markers."""
    if body.count(CHECKLIST_START_MARKER) != 1:
        raise ChecklistError("Reviewer checklist start marker is missing or duplicated")
    if body.count(CHECKLIST_END_MARKER) != 1:
        raise ChecklistError("Reviewer checklist end marker is missing or duplicated")
    start = body.index(CHECKLIST_START_MARKER) + len(CHECKLIST_START_MARKER)
    end = body.index(CHECKLIST_END_MARKER)
    if end < start:
        raise ChecklistError("Reviewer checklist markers are out of order")
    return body[start:end].strip("\n")


def parse_checklist_states(body: str) -> ParsedChecklist:
    """Parse checkbox states by stable ID and reject malformed or duplicate entries."""
    section = extract_checklist_section(body)
    states: dict[str, bool] = {}
    for line in section.splitlines():
        if not line.strip():
            continue
        match = _ITEM_LINE.fullmatch(line)
        if not match:
            raise ChecklistError(f"Malformed reviewer checklist line: {line}")
        checked, _text, item_id = match.groups()
        if item_id in states:
            raise ChecklistError(f"Duplicate reviewer checklist item ID: {item_id}")
        states[item_id] = checked.casefold() == "x"
    return ParsedChecklist(states)


def validate_required_items(states: dict[str, bool]) -> None:
    """Require every configured item to exist and be checked."""
    items = required_items()
    for item in items:
        if item.item_id not in states:
            raise ChecklistError(f"Required checklist item {item.item_id} is missing")
    complete = sum(states[item.item_id] for item in items)
    if complete != len(items):
        raise ChecklistError(
            f"Reviewer checklist incomplete ({complete}/{len(items)} complete)"
        )
