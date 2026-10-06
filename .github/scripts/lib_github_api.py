"""Minimal GitHub REST client and Actions helpers shared by the .github scripts."""

from __future__ import annotations

import json
import os
from functools import cache
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import yaml

_BOT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "pr-bot-comments" / "bot.yml"


class RoutingError(Exception):
    """An expected configuration or GitHub API error."""


class GitHubApiError(RoutingError):
    """An HTTP error returned by the GitHub API."""

    def __init__(self, status: int, detail: str):
        """Store the response status and body for policy decisions."""
        super().__init__(f"GitHub API returned {status}: {detail}")
        self.status = status
        self.detail = detail


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


_MARKER_KEYS = (
    "approval",
    "tracker",
    "tracker_state_prefix",
    "checklist_start",
    "checklist_end",
)


@cache
def bot_config() -> dict:
    """Load the bot identity and comment markers from pr-bot-comments/bot.yml."""
    try:
        raw = yaml.safe_load(_BOT_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise RoutingError(f"Could not load bot config: {error}") from error
    if (
        not isinstance(raw, dict)
        or not isinstance(raw.get("bot_login"), str)
        or not raw["bot_login"]
    ):
        raise RoutingError("Bot config needs a non-empty bot_login string")
    markers = raw.get("markers")
    if not isinstance(markers, dict) or not all(
        isinstance(markers.get(key), str) and markers[key] for key in _MARKER_KEYS
    ):
        raise RoutingError(
            f"Bot config markers must define non-empty strings for: {', '.join(_MARKER_KEYS)}"
        )
    return raw
