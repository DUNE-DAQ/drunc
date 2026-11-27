"""
Add a newly opened pull request to the DRUNC Internal project board and set its fields:
 - Status = In Progress
 - Sprint = the current sprint.

The DUNE-DAQ tracking project (project 5) is handled separately by the centrally managed
workflow track_new_prs.yml.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from datetime import date, datetime, timezone

from common import (
    IN_PROGRESS,
    ORG,
    PROJECT_NUMBER,
    STATUS_FIELD,
    option_matching,
    pull_request_from_event,
    report,
)
from lib_github_api import GitHubApi, RoutingError
from lib_graphql_queries import ADD_ITEM_MUTATION, PROJECT_QUERY, SET_FIELD_MUTATION
from sprint_rollover import sprint_end


def current_sprint(sprints: list[dict], today: date) -> dict | None:
    """
    Return the sprint with start <= today < end, or None.

    Args:
        sprints (list[dict]): The list of sprints to check.
        today (date): The current date to check against sprint dates.

    Returns:
        dict | None: The sprint that covers today, or None if no such sprint exists.
    """
    for sprint in sorted(sprints, key=lambda sprint: sprint["startDate"]):
        if date.fromisoformat(sprint["startDate"]) <= today < sprint_end(sprint):
            return sprint
    return None


def choose_in_progress(field: dict, _today: date) -> tuple[dict, str]:
    """
    Pick the status option ending in "In Progress".

    Args:
        field (dict): The project field containing status options.
        _today (date): The current date (unused for this chooser).

    Returns:
        tuple[dict, str]: The chosen status value and its name.
    """
    option = option_matching(field, lambda name: bool(IN_PROGRESS.search(name)))
    return {"singleSelectOptionId": option["id"]}, option["name"]


def choose_current_sprint(field: dict, today: date) -> tuple[dict, str] | None:
    """
    Pick the sprint that covers today, if any.

    Args:
        field (dict): The project field containing sprint information.
        today (date): The current date to check against sprint dates.

    Returns:
        tuple[dict, str] | None: The chosen sprint value and its title, or None if no
                                    sprint covers today.
    """
    # Get the list of sprints from the field configuration.
    sprints = (field.get("configuration") or {}).get("iterations") or []

    # Find the current sprint that covers today.
    sprint = current_sprint(sprints, today)
    if sprint is None:
        return None

    # Return the chosen sprint value and its title.
    return {"iterationId": sprint["id"]}, sprint["title"]


# Fields to set on the new item, in order. Each chooser returns (value, description) for
# the field, or None if nothing should be set.
FIELDS: tuple[tuple[str, Callable[[dict, date], tuple[dict, str] | None]], ...] = (
    (STATUS_FIELD, choose_in_progress),
    ("Sprint", choose_current_sprint),
)


def fetch_project(api: GitHubApi) -> tuple[str, str, list[dict]]:
    """
    Fetch the project information from GitHub.

    Returns a tuple containing
     - project ID
     - project title
     - list of fields named in FIELDS, in order

    Args:
        api: GitHubApi, the API client to use for GraphQL requests.

    Returns:
        tuple[str, str, list[dict]]: The project ID, title, and the fields named in
                                        FIELDS, in order.
    """

    # Query for the required fields
    names = [name for name, _ in FIELDS]  # status, sprint
    data = api.graphql(
        PROJECT_QUERY,
        {"org": ORG, "num": PROJECT_NUMBER, "f1": names[0], "f2": names[1]},
    )

    # Extract the project information from the response
    project = (data.get("organization") or {}).get("projectV2")
    if not isinstance(project, dict) or not project.get("id"):
        raise RoutingError(
            f"Project {ORG}#{PROJECT_NUMBER} was not found or the token cannot read it"
        )

    # Extract the fields for the project
    fields = [project.get("f1") or {}, project.get("f2") or {}]
    for name, field in zip(names, fields):
        if not field.get("id"):
            raise RoutingError(
                f"Project '{project['title']}' has no usable field named '{name}'"
            )

    # Return the project ID, title, and the extracted fields
    return project["id"], project["title"], fields


def track(api: GitHubApi, content_id: str, today: date) -> list[str]:
    """
    Add the pull request to the project and set its fields. Return summary lines.

    Args:
        api: GitHubApi, the API client to use for GraphQL requests.
        content_id: str, the ID of the pull request content.
        today: date, the current date.

    Returns:
        list[str]: Summary lines describing the actions taken.
    """
    # Get the project ID, title, and fields
    project_id, title, fields = fetch_project(api)

    # Add the pull request to the project
    added = api.graphql(
        ADD_ITEM_MUTATION, {"project": project_id, "content": content_id}
    )

    # Extract the item ID from the response and validate it
    item_id = ((added.get("addProjectV2ItemById") or {}).get("item") or {}).get("id")
    if not item_id:
        raise RoutingError(f"GitHub did not return an item ID for '{title}'")

    # Initialize the summary lines with the project title and added status
    lines = [f"- **{title}**: added"]

    # Set the fields for the pull request in the project
    for (name, choose), field in zip(FIELDS, fields):
        # Determine the choice for the current field based on the date
        choice = choose(field, today)

        # Skip setting the field if no choice is determined for the current date
        if choice is None:
            message = f"no value to set for {name} on {today.isoformat()}"
            print(f"::warning::{title}: {message}")
            lines.append(f"  - {name}: skipped ({message})")
            continue

        # Set the field value for the current choice
        value, label = choice
        api.graphql(
            SET_FIELD_MUTATION,
            {
                "project": project_id,
                "item": item_id,
                "field": field["id"],
                "value": value,
            },
        )
        lines.append(f"  - {name}: {label}")
    return lines


def main() -> int:
    """
    Track the pull request on the DRUNC Internal project.

    Args:
        None

    Returns:
        int: 0 if tracking succeeded, 1 if it failed.
    """
    # Initialize the GitHub API client
    api = GitHubApi.from_environment()

    # Extract the pull request information from the Actions event payload
    pull = pull_request_from_event()

    # Get the current date in UTC timezone
    today = datetime.now(timezone.utc).date()

    # Construct the summary lines for the pull request tracking
    workflow_stdout = [f"### Track new PR #{pull.get('number')}"]

    # Track the pull request
    try:
        workflow_stdout.extend(track(api, pull["node_id"], today))
    except RoutingError as error:
        print(f"::error::Project {ORG}#{PROJECT_NUMBER}: {error}")
        workflow_stdout.append(
            f"- **Project {ORG}#{PROJECT_NUMBER}**: failed ({error})"
        )
        report(workflow_stdout)
        return 1

    # Report the successful tracking
    report(workflow_stdout)

    return 0


if __name__ == "__main__":
    try:
        # Run the main function and exit with its return code. This is needed for the
        # exit code to indicate success or failure to the calling environment.
        sys.exit(main())
    except (KeyError, ValueError, RoutingError) as error:
        print(f"Tracking the pull request failed: {error}", file=sys.stderr)
        sys.exit(1)
