"""Convert a newly opened, ready-for-review pull request to draft."""

from __future__ import annotations

import sys

from lib_github_api import GitHubApi, RoutingError, append_summary


def main() -> int:
    """Look up the pull request and convert it with GitHub's GraphQL API."""
    api = GitHubApi.from_environment()
    pull_number = api.pull_number_from_environment()
    pull_request = api.get(f"pulls/{pull_number}")
    if not isinstance(pull_request, dict):
        raise RoutingError("Expected a pull request object from GitHub")
    if pull_request.get("draft"):
        append_summary(f"### Enforce draft\n- PR #{pull_number} is already a draft.")
        return 0
    node_id = pull_request.get("node_id")
    if not isinstance(node_id, str) or not node_id:
        raise RoutingError("Pull request has no GraphQL node ID")

    data = api.graphql(
        """
        mutation ConvertToDraft($pullRequestId: ID!) {
          convertPullRequestToDraft(input: {pullRequestId: $pullRequestId}) {
            pullRequest { isDraft }
          }
        }
        """,
        {"pullRequestId": node_id},
    )
    conversion = data.get("convertPullRequestToDraft")
    converted_pull = (
        conversion.get("pullRequest") if isinstance(conversion, dict) else None
    )
    if (
        not isinstance(converted_pull, dict)
        or converted_pull.get("isDraft") is not True
    ):
        raise RoutingError("GitHub did not confirm the pull request is a draft")
    append_summary(f"### Enforce draft\n- Converted PR #{pull_number} to draft.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyError, ValueError, RoutingError) as error:
        print(f"Draft conversion failed: {error}", file=sys.stderr)
        sys.exit(1)
