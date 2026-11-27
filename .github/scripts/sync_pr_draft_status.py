"""
Keep a pull request's Status on the DRUNC Internal project board in step with its draft
state:
 - converted to draft -> Status = In progress
 - ready for review   -> Status = Ready for review

The status options are matched by the end of their name, so icons in front of the name
(for example "🏗 In progress") are kept. If the pull request is not on the board yet, it
is added.
"""

from __future__ import annotations

import re
import sys

from common import (
    IN_PROGRESS,
    ORG,
    PROJECT_NUMBER,
    READY_FOR_REVIEW,
    STATUS_FIELD,
    option_matching,
    pull_request_from_event,
    report,
)
from lib_github_api import GitHubApi, RoutingError
from lib_graphql_queries import ADD_ITEM_MUTATION, SET_FIELD_MUTATION, STATUS_QUERY


def target_status(pull: dict) -> re.Pattern:
    """
    Return the pattern of the status option matching the pull request's draft state.

    The draft flag is read from the pull request rather than from the event action, so
    the status always follows the pull request's state when the event was sent.

    Args:
        pull (dict): The pull request data from the event payload.

    Returns:
        re.Pattern: IN_PROGRESS for a draft, READY_FOR_REVIEW otherwise.
    """
    return IN_PROGRESS if pull.get("draft") else READY_FOR_REVIEW


def fetch_status_field(api: GitHubApi) -> tuple[str, str, dict]:
    """
    Fetch the project and its Status field from GitHub.

    Args:
        api (GitHubApi): The API client to use for GraphQL requests.

    Returns:
        tuple[str, str, dict]: The project ID, title and Status field.
    """
    # Query for the Status field
    data = api.graphql(
        STATUS_QUERY, {"org": ORG, "num": PROJECT_NUMBER, "field": STATUS_FIELD}
    )

    # Extract the project information from the response
    project = (data.get("organization") or {}).get("projectV2")
    if not isinstance(project, dict) or not project.get("id"):
        raise RoutingError(
            f"Project {ORG}#{PROJECT_NUMBER} was not found or the token cannot read it"
        )

    # Extract and validate the Status field
    field = project.get("field") or {}
    if not field.get("id"):
        raise RoutingError(
            f"Project '{project['title']}' has no single-select field named "
            f"'{STATUS_FIELD}'"
        )

    # Return the project ID, title, and the Status field
    return project["id"], project["title"], field


def sync_status(api: GitHubApi, content_id: str, pattern: re.Pattern) -> list[str]:
    """
    Set the pull request's Status to the option matching `pattern`. Return summary lines.

    Args:
        api (GitHubApi): The API client to use for GraphQL requests.
        content_id (str): The node ID of the pull request.
        pattern (re.Pattern): The pattern the status option name must match.

    Returns:
        list[str]: Summary lines describing the actions taken.
    """
    # Get the project ID, title, and Status field
    project_id, title, field = fetch_status_field(api)

    # Pick the status option before changing anything, so a missing option does not
    # leave a half-updated item behind
    option = option_matching(field, lambda name: bool(pattern.search(name)))

    # Get the pull request's item on the project, adding it if it is not there yet
    added = api.graphql(
        ADD_ITEM_MUTATION, {"project": project_id, "content": content_id}
    )

    # Extract the item ID from the response and validate it
    item_id = ((added.get("addProjectV2ItemById") or {}).get("item") or {}).get("id")
    if not item_id:
        raise RoutingError(f"GitHub did not return an item ID for '{title}'")

    # Set the Status field to the chosen option
    api.graphql(
        SET_FIELD_MUTATION,
        {
            "project": project_id,
            "item": item_id,
            "field": field["id"],
            "value": {"singleSelectOptionId": option["id"]},
        },
    )

    return [f"- **{title}**", f"  - {STATUS_FIELD}: {option['name']}"]


def main() -> int:
    """
    Sync the pull request's Status on the DRUNC Internal project with its draft state.

    Args:
        None

    Returns:
        int: 0 if the status was set, 1 if it failed.
    """
    # Initialize the GitHub API client
    api = GitHubApi.from_environment()

    # Extract the pull request information from the Actions event payload
    pull = pull_request_from_event()

    # Construct the summary lines for the status sync
    state = "draft" if pull.get("draft") else "ready for review"
    workflow_stdout = [f"### Sync status of PR #{pull.get('number')} ({state})"]

    # Set the status matching the draft state
    try:
        workflow_stdout.extend(sync_status(api, pull["node_id"], target_status(pull)))
    except RoutingError as error:
        print(f"::error::Project {ORG}#{PROJECT_NUMBER}: {error}")
        workflow_stdout.append(
            f"- **Project {ORG}#{PROJECT_NUMBER}**: failed ({error})"
        )
        report(workflow_stdout)
        return 1

    # Report the successful sync
    report(workflow_stdout)

    return 0


if __name__ == "__main__":
    try:
        # Run the main function and exit with its return code. This is needed for the
        # exit code to indicate success or failure to the calling environment.
        sys.exit(main())
    except (KeyError, ValueError, RoutingError) as error:
        print(f"Syncing the pull request status failed: {error}", file=sys.stderr)
        sys.exit(1)
