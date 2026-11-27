# """
# Add a newly opened pull request to the DRUNC Internal project board and set its fields:
#  - Status = In Progress
#  - Sprint = the current sprint.

# The DUNE-DAQ tracking project (project 5) is handled separately by the centrally managed
# workflow track_new_prs.yml.
# """

# from __future__ import annotations

# import json
# import os
# import re
# import sys
# from collections.abc import Callable
# from datetime import date, datetime, timezone
# from pathlib import Path

# from lib_github_api import GitHubApi, RoutingError, append_summary
# from sprint_rollover import sprint_end

# ORG = "DUNE-DAQ"

# # Retrieve a project's ID and two named fields. Fields that do not exist (or are
# # of a different type) come back as null or as an empty object.
# PROJECT_QUERY = """
# query($org: String!, $num: Int!, $f1: String!, $f2: String!) {
#   organization(login: $org) {
#     projectV2(number: $num) {
#       id
#       title
#       f1: field(name: $f1) { ...FieldInfo }
#       f2: field(name: $f2) { ...FieldInfo }
#     }
#   }
# }
# fragment FieldInfo on ProjectV2FieldConfiguration {
#   ... on ProjectV2SingleSelectField { id name options { id name } }
#   ... on ProjectV2IterationField {
#     id name configuration { iterations { id title startDate duration } }
#   }
# }
# """

# # Add the pull request to a project. Returns the existing item if it is already there.
# ADD_ITEM_MUTATION = """
# mutation($project: ID!, $content: ID!) {
#   addProjectV2ItemById(input: {projectId: $project, contentId: $content}) {
#     item { id }
#   }
# }
# """

# # Set one field on a project item. `value` is either {singleSelectOptionId: ...} or
# # {iterationId: ...}.
# SET_FIELD_MUTATION = """
# mutation($project: ID!, $item: ID!, $field: ID!,
#          $value: ProjectV2FieldValue!) {
#   updateProjectV2ItemFieldValue(input: {
#     projectId: $project, itemId: $item, fieldId: $field, value: $value
#   }) { projectV2Item { id } }
# }
# """

# # Matches "In Progress", "In progress", "🏗 In progress", ...
# IN_PROGRESS = re.compile(r"in progress$", re.IGNORECASE)

# PROJECT_NUMBER = 8  # DRUNC Internal

# # Fields to set on the new item, in order. Each chooser returns (value, description) for
# # the field, or None if nothing should be set.
# FIELDS: tuple[tuple[str, Callable[[dict, date], tuple[dict, str] | None]], ...] = (
#     ("Status", choose_in_progress),
#     ("Sprint", choose_current_sprint),
# )


# def option_matching(field: dict, predicate: Callable[[str], bool]) -> dict:
#     """
#     Return the first single-select option whose name satisfies `predicate`.

#     Args:
#         field (dict): The project field containing single-select options.
#         predicate (Callable[[str], bool]): A function to test option names.

#     Returns:
#         dict: The first option that satisfies the predicate.
#     """
#     for option in field.get("options") or []:
#         if predicate(option["name"]):
#             return option
#     names = ", ".join(repr(option["name"]) for option in field.get("options") or [])
#     raise RoutingError(f"No matching option in '{field['name']}' (options: {names})")


# def current_sprint(sprints: list[dict], today: date) -> dict | None:
#     """
#     Return the sprint with start <= today < end, or None.

#     Args:
#         sprints (list[dict]): The list of sprints to check.
#         today (date): The current date to check against sprint dates.

#     Returns:
#         dict | None: The sprint that covers today, or None if no such sprint exists.
#     """
#     for sprint in sorted(sprints, key=lambda sprint: sprint["startDate"]):
#         if date.fromisoformat(sprint["startDate"]) <= today < sprint_end(sprint):
#             return sprint
#     return None


# def choose_in_progress(field: dict, _today: date) -> tuple[dict, str]:
#     """
#     Pick the status option ending in "In Progress".

#     Args:
#         field (dict): The project field containing status options.
#         _today (date): The current date (unused for this chooser).

#     Returns:
#         tuple[dict, str]: The chosen status value and its name.
#     """
#     option = option_matching(field, lambda name: bool(IN_PROGRESS.search(name)))
#     return {"singleSelectOptionId": option["id"]}, option["name"]


# def choose_current_sprint(field: dict, today: date) -> tuple[dict, str] | None:
#     """
#     Pick the sprint that covers today, if any.

#     Args:
#         field (dict): The project field containing sprint information.
#         today (date): The current date to check against sprint dates.

#     Returns:
#         tuple[dict, str] | None: The chosen sprint value and its title, or None if no
#                                     sprint covers today.
#     """
#     # Get the list of sprints from the field configuration.
#     sprints = (field.get("configuration") or {}).get("iterations") or []

#     # Find the current sprint that covers today.
#     sprint = current_sprint(sprints, today)
#     if sprint is None:
#         return None

#     # Return the chosen sprint value and its title.
#     return {"iterationId": sprint["id"]}, sprint["title"]


# def pull_request_from_event() -> dict:
#     """
#     Read the pull request from the Actions event payload.

#     Args:
#         None

#     Returns:
#         dict: The pull request data from the event payload.
#     """
#     # Read the event payload from the file specified by GITHUB_EVENT_PATH.
#     event_path = os.environ.get("GITHUB_EVENT_PATH")
#     if not event_path:
#         raise RoutingError("GITHUB_EVENT_PATH is required")

#     # Load the event payload from the file.
#     event = json.loads(Path(event_path).read_text(encoding="utf-8"))

#     # Extract the pull request from the event payload.
#     pull = event.get("pull_request")
#     if not isinstance(pull, dict) or not pull.get("node_id"):
#         raise RoutingError("The event has no pull request node ID")

#     return pull


# def fetch_project(api: GitHubApi) -> tuple[str, str, list[dict]]:
#     """
#     Fetch the project information from GitHub.

#     Args:
#         api: GitHubApi, the API client to use for GraphQL requests.

#     Returns:
#         tuple[str, str, list[dict]]: The project ID, title, and the fields named in
#                                         FIELDS, in order.
#     """
#     """Return the project ID, title and the fields named in FIELDS, in order."""
#     names = [name for name, _ in FIELDS]
#     data = api.graphql(
#         PROJECT_QUERY,
#         {"org": ORG, "num": PROJECT_NUMBER, "f1": names[0], "f2": names[1]},
#     )
#     project = (data.get("organization") or {}).get("projectV2")
#     if not isinstance(project, dict) or not project.get("id"):
#         raise RoutingError(
#             f"Project {ORG}#{PROJECT_NUMBER} was not found or the token cannot read it"
#         )
#     fields = [project.get("f1") or {}, project.get("f2") or {}]
#     for name, field in zip(names, fields):
#         if not field.get("id"):
#             raise RoutingError(
#                 f"Project '{project['title']}' has no usable field named '{name}'"
#             )
#     return project["id"], project["title"], fields


# def track(api: GitHubApi, content_id: str, today: date) -> list[str]:
#     """
#     Add the pull request to the project and set its fields. Return summary lines.

#     Args:
#         api: GitHubApi, the API client to use for GraphQL requests.
#         content_id: str, the ID of the pull request content.
#         today: date, the current date.

#     Returns:
#         list[str]: Summary lines describing the actions taken.
#     """
#     # Get the project ID, title, and fields
#     project_id, title, fields = fetch_project(api)

#     # Add the pull request to the project
#     added = api.graphql(
#         ADD_ITEM_MUTATION, {"project": project_id, "content": content_id}
#     )

#     # Extract the item ID from the response and validate it
#     item_id = ((added.get("addProjectV2ItemById") or {}).get("item") or {}).get("id")
#     if not item_id:
#         raise RoutingError(f"GitHub did not return an item ID for '{title}'")

#     # Initialize the summary lines with the project title and added status
#     lines = [f"- **{title}**: added"]

#     # Set the fields for the pull request in the project
#     for (name, choose), field in zip(FIELDS, fields):
#         # Determine the choice for the current field based on the date
#         choice = choose(field, today)

#         # Skip setting the field if no choice is determined for the current date
#         if choice is None:
#             message = f"no value to set for {name} on {today.isoformat()}"
#             print(f"::warning::{title}: {message}")
#             lines.append(f"  - {name}: skipped ({message})")
#             continue

#         # Set the field value for the current choice
#         value, label = choice
#         api.graphql(
#             SET_FIELD_MUTATION,
#             {
#                 "project": project_id,
#                 "item": item_id,
#                 "field": field["id"],
#                 "value": value,
#             },
#         )
#         lines.append(f"  - {name}: {label}")
#     return lines


# def main() -> int:
#     """
#     Track the pull request on the DRUNC Internal project.

#     Args:
#         None

#     Returns:
#         int: 0 if tracking succeeded, 1 if it failed.
#     """
#     # Initialize the GitHub API client
#     api = GitHubApi.from_environment()

#     # Extract the pull request information from the event payload
#     pull = pull_request_from_event()

#     # Get the current date in UTC timezone
#     today = datetime.now(timezone.utc).date()

#     # Construct the summary lines for the pull request tracking
#     workflow_stdout = [f"### Track new PR #{pull.get('number')}"]

#     # Track the pull request
#     try:
#         workflow_stdout.extend(track(api, pull["node_id"], today))
#     except RoutingError as error:
#         print(f"::error::Project {ORG}#{PROJECT_NUMBER}: {error}")
#         workflow_stdout.append(
#             f"- **Project {ORG}#{PROJECT_NUMBER}**: failed ({error})"
#         )
#         return 1

#     # Print the summary of the pull request tracking
#     summary = "\n".join(workflow_stdout)
#     print(summary)

#     # Append the summary to the workflow summary section
#     append_summary(summary)

#     return 0


# if __name__ == "__main__":
#     try:
#         # Run the main function and exit with its return code. This is needed for the
#         # exit code to indicate success or failure to the calling environment.
#         sys.exit(main())
#     except (KeyError, ValueError, RoutingError) as error:
#         print(f"Tracking the pull request failed: {error}", file=sys.stderr)
#         sys.exit(1)
