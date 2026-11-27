"""
Move unfinished project items from the previous sprint to the current sprint.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone

from lib_github_api import GitHubApi, RoutingError, append_summary

# GraphQL queries and mutations for interacting with GitHub project sprints.
## Retrieve project field information
FIELD_QUERY = """
query($org: String!, $num: Int!, $name: String!) {
  organization(login: $org) {
    projectV2(number: $num) {
      id
      field(name: $name) {
        ... on ProjectV2IterationField {
          id
          configuration {
            iterations { id title startDate duration }
            completedIterations { id title startDate duration }
          }
        }
      }
    }
  }
}
"""

## Retrieve project items for a given sprint field
ITEMS_QUERY = """
query($project: ID!, $name: String!, $after: String) {
  node(id: $project) {
    ... on ProjectV2 {
      items(first: 100, after: $after) {
        nodes {
          id
          isArchived
          content {
            ... on Issue { title url }
            ... on PullRequest { title url }
            ... on DraftIssue { title }
          }
          status: fieldValueByName(name: "Status") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          sprint: fieldValueByName(name: $name) {
            ... on ProjectV2ItemFieldIterationValue { iterationId }
          }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}
"""

## Move an item to a different sprint iteration
MOVE_MUTATION = """
mutation($project: ID!, $item: ID!, $field: ID!, $iteration: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $project, itemId: $item, fieldId: $field,
    value: { iterationId: $iteration }
  }) { projectV2Item { id } }
}
"""


def sprint_end(sprint: dict) -> date:
    """Return the exclusive end date of a sprint (start + duration days)."""
    return date.fromisoformat(sprint["startDate"]) + timedelta(days=sprint["duration"])


def pick_sprints(sprints: list[dict], today: date) -> tuple[dict, dict] | str:
    """
    Return (previous, current) sprints for today, or a reason nothing should move.

    The current sprint is the one with start <= today < end. The previous sprint is the
    most recent one that has already ended (end <= today).

    Args:
        sprints: list[dict], the list of all sprints (past and upcoming).
        today: date, the current date to determine the active sprint.

    Returns:
        tuple[dict, dict] | str: A tuple of (previous, current) sprints if found,
                                    otherwise a string explaining why nothing should move.
    """

    # Sort sprints by start date to determine the current and previous sprints.
    ordered = sorted(sprints, key=lambda sprint: sprint["startDate"])

    # Find the current sprint that includes today.
    current = next(
        (
            sprint
            for sprint in ordered
            if date.fromisoformat(sprint["startDate"]) <= today < sprint_end(sprint)
        ),
        None,
    )
    if current is None:
        return f"No sprint covers {today.isoformat()}."

    # Find all sprints that have ended by today.
    ended = [sprint for sprint in ordered if sprint_end(sprint) <= today]
    if not ended:
        return "No previous sprint has ended yet."

    # Return the most recent ended sprint and the current sprint.
    return ended[-1], current


def is_unfinished(item: dict) -> bool:
    """
    Treat an item as unfinished unless its Status contains "Done".

    Args:
        item: dict, the item to check.

    Returns:
        bool: True if the item is unfinished, False otherwise.
    """
    status = (item.get("status") or {}).get("name") or ""
    return "Done" not in status


def items_to_move(items: list[dict], previous_id: str) -> list[dict]:
    """
    Select unarchived, unfinished items assigned to the previous sprint.

    Args:
        items: list[dict], the list of all items to filter.
        previous_id: str, the ID of the previous sprint.

    Returns:
        list[dict]: The list of items to move to the current sprint.
    """
    return [
        item
        for item in items
        if not item.get("isArchived")
        and (item.get("sprint") or {}).get("iterationId") == previous_id
        and is_unfinished(item)
    ]


def item_label(item: dict) -> str:
    """Describe an item for logs and the step summary."""
    content = item.get("content") or {}
    title = content.get("title") or item["id"]
    url = content.get("url")
    return f"[{title}]({url})" if url else title


def fetch_sprint_field(
    api: GitHubApi, org: str, number: int, field_name: str
) -> tuple[str, str, list[dict]]:
    """
    Return the project ID, sprint field ID and all sprints (past and upcoming).

    Args:
        api: GitHubApi, the API client to use for GraphQL requests.
        org: str, the organization name.
        number: int, the project number.
        field_name: str, the name of the sprint field.

    Returns:
        tuple[str, str, list[dict]]: The project ID, sprint field ID, and all sprints (past and upcoming).

    """
    # Fetch the project and sprint field information from GitHub.
    data = api.graphql(FIELD_QUERY, {"org": org, "num": number, "name": field_name})

    # Extract the project information from the API response.
    project = (data.get("organization") or {}).get("projectV2")
    if not isinstance(project, dict) or not project.get("id"):
        raise RoutingError(
            f"Project {org}#{number} was not found or the token cannot read it"
        )

    # Extract the sprint field information from the project.
    field = project.get("field") or {}
    if not field.get("id"):
        raise RoutingError(
            f"Project {org}#{number} has no iteration field named '{field_name}'"
        )

    # Extract the sprint configuration from the field.
    configuration = field.get("configuration") or {}
    sprints = configuration.get("completedIterations", []) + configuration.get(
        "iterations", []
    )

    # Return the project ID, sprint field ID, and all sprints (past and upcoming).
    return project["id"], field["id"], sprints


def fetch_items(api: GitHubApi, project_id: str, field_name: str) -> list[dict]:
    """
    Fetch every item on the project, following GraphQL pagination.

    Args:
        api: GitHubApi, the API client to use for GraphQL requests.
        project_id: str, the ID of the project.
        field_name: str, the name of the sprint field.

    Returns:
        list[dict]: The list of all items on the project.
    """

    # Initialize the list of items and the pagination cursor.
    items: list[dict] = []
    after: str | None = None

    # Fetch items page by page until all are retrieved.
    while True:
        # Request the next page of items from the API.
        data = api.graphql(
            ITEMS_QUERY, {"project": project_id, "name": field_name, "after": after}
        )

        # Extract the page of items from the API response.
        page = ((data.get("node") or {}).get("items")) or {}

        # Add the items from this page to the list of all items.
        items.extend(page.get("nodes") or [])

        # Check if there is another page of items to fetch.
        page_info = page.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            return items

        # Update the pagination cursor for the next page.
        after = page_info.get("endCursor")
        if not after:
            raise RoutingError("GitHub reported another page but gave no cursor")


def move_item(
    api: GitHubApi, project_id: str, field_id: str, item_id: str, iteration_id: str
) -> None:
    """
    Assign one project item to the given sprint.

    Args:
        api: GitHubApi, the API client to use for GraphQL requests.
        project_id: str, the ID of the project.
        field_id: str, the ID of the sprint field.
        item_id: str, the ID of the project item.
        iteration_id: str, the ID of the sprint iteration.

    Returns:
        None
    """
    api.graphql(
        MOVE_MUTATION,
        {
            "project": project_id,
            "item": item_id,
            "field": field_id,
            "iteration": iteration_id,
        },
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    Read options from the command line, falling back to the environment.

    Args:
        argv: list[str] | None, the command-line arguments to parse.
    Returns:
        argparse.Namespace: The parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--org", default=os.environ.get("ORG", "DUNE-DAQ"))
    parser.add_argument(
        "--project-number",
        type=int,
        default=int(os.environ.get("PROJECT_NUMBER", "8")),
    )
    parser.add_argument(
        "--sprint-field", default=os.environ.get("SPRINT_FIELD", "Sprint")
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.environ.get("DRY_RUN", "").lower() == "true",
        help="List the items that would move without changing anything.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """
    Move unfinished items from the previous sprint into the current one.
    """

    # Parse command-line arguments and set up the API client.
    args = parse_args(argv)
    api = GitHubApi.from_environment()

    # Determine today's date in UTC.
    today = datetime.now(timezone.utc).date()

    # Fetch the project ID, sprint field ID, and all sprints.
    project_id, field_id, sprints = fetch_sprint_field(
        api, args.org, args.project_number, args.sprint_field
    )

    # Pick the previous and current sprints based on today's date.
    picked = pick_sprints(sprints, today)

    # If no suitable sprints are found, log the reason.
    if isinstance(picked, str):
        print(f"::warning::{picked} Nothing changed.")
        append_summary(f"### Sprint rollover\n- {picked} Nothing changed.")
        return 0

    # Unpack the previous and current sprints.
    previous, current = picked

    # Determine which items need to be moved from the previous sprint to the current one.
    to_move = items_to_move(
        fetch_items(api, project_id, args.sprint_field), previous["id"]
    )

    # Prepare the summary lines for the sprint rollover.
    verb = "Would move" if args.dry_run else "Moved"
    workflow_stdout = [
        f"### Sprint rollover{' (dry run)' if args.dry_run else ''}",
        f"- From: {previous.get('title', previous['id'])}",
        f"- To: {current.get('title', current['id'])}",
        f"- {verb} {len(to_move)} item(s)",
    ]

    # Move each item to the current sprint and update the summary.
    for item in to_move:
        if not args.dry_run:
            move_item(api, project_id, field_id, item["id"], current["id"])
        print(f"{verb} {item_label(item)}")
        workflow_stdout.append(f"  - {item_label(item)}")

    # Append the summary lines to the overall summary.
    append_summary("\n".join(workflow_stdout))

    # Log the completion of the sprint rollover.
    print("Sprint rollover completed.")
    return 0


if __name__ == "__main__":
    try:
        # Run the main function and exit with its return code. This is needed for the
        # exit code to indicate success or failure to the calling environment.
        sys.exit(main())
    except (KeyError, ValueError, RoutingError) as error:
        print(f"Sprint rollover failed: {error}", file=sys.stderr)
        sys.exit(1)
