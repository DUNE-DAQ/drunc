"""Explain to the author why their newly opened PR was converted to draft."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from lib_github_api import GitHubApi, RoutingError, append_summary


def main() -> int:
    """Post the opened-as-ready template on the pull request."""
    token = os.environ.get("GITHUB_TOKEN")
    repository = os.environ.get("GITHUB_REPOSITORY")
    if not token or not repository:
        raise RoutingError("GITHUB_TOKEN and GITHUB_REPOSITORY are required")
    pull_number = int(os.environ["PR_NUMBER"])

    template_path = (
        Path(__file__).resolve().parents[1]
        / "pr-bot-comments"
        / "opened-as-ready-comment.md"
    )
    try:
        body = template_path.read_text(encoding="utf-8").strip()
    except OSError as error:
        raise RoutingError(
            f"Could not load opened-as-ready comment: {error}"
        ) from error

    GitHubApi(repository, token).request(
        "POST", f"issues/{pull_number}/comments", {"body": body}
    )
    append_summary("### Enforce draft\n- Converted to draft and commented.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyError, ValueError, RoutingError) as error:
        print(f"Opened-as-ready comment failed: {error}", file=sys.stderr)
        sys.exit(1)
