"""Post the approval comment once per pull request."""

from __future__ import annotations

import sys
from pathlib import Path

from lib_github_api import GitHubApi, RoutingError, append_summary, bot_config


def main() -> int:
    """Post the approval template unless the bot has already posted it."""
    api = GitHubApi.from_environment()
    pull_number = api.pull_number_from_environment()
    marker = bot_config()["markers"]["approval"]
    bot_login = bot_config()["bot_login"]

    comments = api.paginated(f"issues/{pull_number}/comments")
    if any(
        isinstance(comment.get("body"), str)
        and marker in comment["body"]
        and isinstance(comment.get("user"), dict)
        and comment["user"].get("login") == bot_login
        for comment in comments
    ):
        append_summary("### Approval comment\n- Already posted; skipping.")
        return 0

    template_path = (
        Path(__file__).resolve().parents[1] / "pr-bot-comments" / "approval-comment.md"
    )
    try:
        template = template_path.read_text(encoding="utf-8").strip()
    except OSError as error:
        raise RoutingError(f"Could not load approval comment: {error}") from error

    api.request(
        "POST",
        f"issues/{pull_number}/comments",
        {"body": f"{marker}\n{template}"},
    )
    append_summary("### Approval comment\n- Posted.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyError, ValueError, RoutingError) as error:
        print(f"Approval comment failed: {error}", file=sys.stderr)
        sys.exit(1)
