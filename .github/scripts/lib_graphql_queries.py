"""
GraphQL queries and mutations used by the .github scripts to read and update GitHub
Projects.

The queries are grouped by the area they are used for. Shared item operations are listed
first because several scripts use them.
"""

# --- Project items (shared) -------------------------------------------------------

# Add the pull request to a project. Returns the existing item if it is already there.
ADD_ITEM_MUTATION = """
mutation($project: ID!, $content: ID!) {
  addProjectV2ItemById(input: {projectId: $project, contentId: $content}) {
    item { id }
  }
}
"""

# Set one field on a project item. `value` is either {singleSelectOptionId: ...} or
# {iterationId: ...}.
SET_FIELD_MUTATION = """
mutation($project: ID!, $item: ID!, $field: ID!,
         $value: ProjectV2FieldValue!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $project, itemId: $item, fieldId: $field, value: $value
  }) { projectV2Item { id } }
}
"""

# --- Pull request tracking --------------------------------------------------------

# Retrieve a project's ID and two named fields. Fields that do not exist (or are
# of a different type) come back as null or as an empty object.
PROJECT_QUERY = """
query($org: String!, $num: Int!, $f1: String!, $f2: String!) {
  organization(login: $org) {
    projectV2(number: $num) {
      id
      title
      f1: field(name: $f1) { ...FieldInfo }
      f2: field(name: $f2) { ...FieldInfo }
    }
  }
}
fragment FieldInfo on ProjectV2FieldConfiguration {
  ... on ProjectV2SingleSelectField { id name options { id name } }
  ... on ProjectV2IterationField {
    id name configuration { iterations { id title startDate duration } }
  }
}
"""

# --- Draft status sync ------------------------------------------------------------

# Retrieve a project's ID and its Status field. A missing field (or one of a different
# type) comes back as null or as an empty object.
STATUS_QUERY = """
query($org: String!, $num: Int!, $field: String!) {
  organization(login: $org) {
    projectV2(number: $num) {
      id
      title
      field(name: $field) {
        ... on ProjectV2SingleSelectField { id name options { id name } }
      }
    }
  }
}
"""

# --- Sprint rollover --------------------------------------------------------------

# Retrieve project field information
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

# Retrieve project items for a given sprint field
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

# Move an item to a different sprint iteration
MOVE_MUTATION = """
mutation($project: ID!, $item: ID!, $field: ID!, $iteration: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $project, itemId: $item, fieldId: $field,
    value: { iterationId: $iteration }
  }) { projectV2Item { id } }
}
"""
