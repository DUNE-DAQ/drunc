

# Dependency-aware nightly setup and reusable package actions

## Other human suggestion (that hasn't been propagated to the rest of the document)

Later on for the choosing which repositories to include in the build, follow this heiarchy:
- follow exactly all the things that is asked for in the dependency-pr bit
- If its not explicitly asked for, get anyway the following repositories
  - drunc (pythoncode)
  - druncschema (sourcecode)
  - daqpytools(pythoncoode)
- For the above, go for the exact same branch name as in the current PR. If that branch name doesnt exist, then use develop. 

- We also want an extra trigger word to retrigger all of the integgtests in the CI. this is similar to the /retry-checklist that already exissts in the #1076 PR, but its a new command that retriggers a full workflow rather than an individual job (it should re-resolve the dependencies and grab everything, basically like running a new pipeline of that job)


- it should be possible to check if something is a python package or if its not. Possibly by checking to see `    if [ -f "pyproject.toml" ] || [ -f "setup.py" ]; then`. If its a python one, install in pythoncode, if its not installs in sourcecode. 

## Summary

Refactor the nightly setup used by `DUNE-DAQ/drunc` so that:

1. Package setup is reusable for Python packages and DBT source packages.
2. Authors can declare dependency PRs in a marked section of the main PR description.
3. CI resolves those PRs to their current head commits whenever the setup/build job runs.
4. The selected revisions are published in the Actions job summary and saved with the nightly artifact.

Implement this incrementally using the subtasks below.

## Context

Related work: `DUNE-DAQ/drunc#1076`, especially:

- `.github/actions/setup-dev-nightly-with-drunc/action.yml`
- `.github/workflows/testing.yml`
- `.github/pull_request_template.md`

The action inspected during design:

- Creates and initializes a nightly DBT workarea.
- Clones `druncschema` into `sourcecode`.
- Clones `drunc` into `pythoncode`.
- Tries the same owner and branch name for both repositories, falling back to upstream.
- Runs Python installation for both packages.
- Builds the workarea and packages it for downstream test jobs.

The new implementation must not require dependency branches to have the same name or owner as the main PR branch.

Because `druncschema` also currently has a Python installation step, distinguish **DBT source packages** from **Python packages**, rather than assuming source packages never require Python installation.

Reinspect the implementation when starting work: the referenced PR may have changed since this specification was written.

## Goals

- Accept explicit dependency PR references.
- Support dependency PRs from forks without requiring matching branch names.
- Separate dependency selection from checkout, installation, and build.
- Resolve dependencies once per build attempt and use the resulting SHAs consistently.
- Provide understandable validation failures.
- Make the tested combination visible without posting PR comments.
- Preserve existing nightly artifact and downstream test behaviour unless explicitly changed.

## Non-goals

For the initial implementation:

- No build triggered by editing a PR description.
- No automatic triggering when a dependency PR receives commits.
- No recursive parsing of dependency PR descriptions.
- No dependency graph discovery or conflict solving across repositories.
- No arbitrary installation commands supplied through PR descriptions.
- No automatic upstream fallback for an explicitly declared dependency.
- No PR comments for dependency reporting.
- No new GitHub repository or issue creation.
- No unrelated workflow cleanup.

## 1. PR template contract

Add or replace the existing dependency section with:

```markdown
## Dependency PRs for CI

List dependency PRs that must be included when building the test workarea.
Use one PR reference per bullet, either a full GitHub PR URL or
`owner/repository#123`.

Leave the block empty if no dependency overrides are needed.
Unlisted packages retain their configured baseline versions.

<!-- dependency-prs:start:v1 -->
<!-- dependency-prs:end:v1 -->
```

Do not include active example dependencies in the default block.

An illustrative completed block:

```markdown
<!-- dependency-prs:start:v1 -->
- DUNE-DAQ/druncschema#123
- https://github.com/DUNE-DAQ/daqpytools/pull/456
<!-- dependency-prs:end:v1 -->
```

The numbers above are examples, not actual dependencies.

### Marker rules

- Parse only the content between the exact, versioned markers.
- Require markers on their own lines, allowing surrounding whitespace.
- Exactly one start marker and one end marker are permitted.
- The start marker must precede the end marker.
- Neither marker present: use no overrides and note this in the summary for compatibility with older PR descriptions.
- Both markers present with an empty block: use no overrides.
- Only one marker present: fail validation.
- Duplicate or reversed markers: fail validation.
- Ignore unrelated text and references outside the block.

Example error:

> Dependency section is incomplete: found the start marker but no end marker. Restore the missing marker.

## 2. Reference parser contract

Require one reference per bullet.

Accept these forms:

```text
- DUNE-DAQ/druncschema#123
- https://github.com/DUNE-DAQ/druncschema/pull/123
- [Schema changes](https://github.com/DUNE-DAQ/druncschema/pull/123)
- <https://github.com/DUNE-DAQ/druncschema/pull/123>
```

### Parsing rules

- Accept standard unordered bullet prefixes: `-`, `*`, or `+`.
- Ignore blank lines inside the block.
- Reject every other nonempty line unless it contains one valid bullet/reference.
- Require a positive PR number.
- Reject bare `#123`: a dependency must identify its repository explicitly.
- Reject trailing prose, multiple references on one line, and nested lists.
- Accept HTTPS GitHub PR URLs only.
- Permit an optional trailing slash and URL query/fragment suffixes.
- Reject unrelated paths, such as issue pages or `/pull/123/files`.
- Do not implement a general-purpose Markdown parser unless necessary.
- Do not execute or evaluate parsed content.

Normalize accepted input into:

```json
{
  "owner": "DUNE-DAQ",
  "repository": "druncschema",
  "number": 123
}
```

Compare repository identities case-insensitively for allowlist and duplicate checks.

### Validation rules

- Reject unsupported target repositories.
- Reject multiple entries for the same target repository, even identical duplicates.
- Reject the main repository under test as a dependency override.
- Include the offending line and a useful explanation in validation errors.
- Never silently discard malformed dependency declarations.

The shorthand identifies a potential issue/PR reference. The resolver must confirm that it represents an accessible pull request.

## 3. Trusted package configuration

Maintain a repository-controlled registry of supported packages.

Each entry must define the information needed to:

- Identify the allowed target repository.
- Select the setup strategy: Python package or DBT source package.
- Choose a fixed destination under `pythoncode` or `sourcecode`.
- Determine whether Python installation is needed.
- Select reviewed installation options/extras.
- Determine baseline behaviour when there is no override.
- Determine installation order when relevant.

Initial expected entries:

- `DUNE-DAQ/drunc`
- `DUNE-DAQ/druncschema`
- `DUNE-DAQ/daqpytools`

Verify the actual setup requirements for each package before finalizing entries, particularly `daqpytools`.

### Baseline policy

Unlisted dependencies retain their configured baseline; do not infer a same-named branch.

For each package, explicitly document whether its baseline is:

- The version already supplied by the nightly, or
- A configured upstream revision that must be checked out locally.

Do not assume the nightly contains source checkouts for every package.

If a baseline requires a checkout, resolve it to a SHA for the build manifest.

Preserve existing required package availability while making any intentional baseline change explicit.

Package destinations and executable setup behaviour must not come from the PR description.

## 4. Dependency resolution

### Main PR description

Whenever the setup/build job runs for a PR:

1. Fetch the current main PR metadata.
2. Read its current `body`, treating a null body as empty text.
3. Parse and validate the marked dependency block.
4. Resolve each requested dependency.
5. Write one resolved build manifest.

Fetch the current description rather than relying exclusively on the original event payload, because reruns must see the current dependency list.

For a push without PR context, do not guess an associated PR. Use configured dependency baselines and the workflow-selected main revision.

### API requests

Fetch the main PR:

```text
GET /repos/DUNE-DAQ/drunc/pulls/{main_pr_number}
```

For each declared dependency:

```text
GET /repos/{owner}/{repository}/pulls/{number}
```

Extract and validate:

- PR state.
- Base repository identity.
- Head repository identity.
- Head branch.
- Head commit SHA.

The declared target repository must be allowlisted, and the returned base repository must match it.

The head repository may be a contributor fork. Do not require it to belong to the target organization.

### Revision selection

- Use each dependency PR's current `head.sha`.
- Use the returned head repository as its source.
- Preserve the branch name for reporting, not as the final checkout identity.
- Initially accept only open dependency PRs.
- Reject closed/merged PRs, deleted/unavailable head repositories, inaccessible PRs, and malformed API responses.
- Do not silently fall back if a declared dependency fails.
- Use bounded timeouts and bounded retries for transient failures only.
- Do not treat authentication or validation failures as transient fallback conditions.

### Main repository revision

Keep the main revision distinct from dependency resolution:

- Use the revision selected by the triggering workflow.
- Explicitly preserve/document whether this is a PR head or test merge revision.
- Do not fetch the latest main PR head and silently replace the workflow-selected revision during a rerun.
- Do not fall back to upstream if checkout of the selected main revision fails.

### Resolution consistency

Resolve dependency selections once per setup/build attempt.

All checkout, installation, artifact creation, and downstream testing must use that manifest.

If a dependency changes after resolution, it must not change the current build's selected revision.

## 5. Trigger and rerun behaviour

| Event | Required behaviour |
|---|---|
| New commit on main PR | Existing eligible setup/build runs and resolves dependencies afresh |
| PR marked ready for review | Existing eligible setup/build runs and resolves dependencies afresh |
| PR description edited | No dependency build |
| Dependency PR updated | No automatic main-PR build |
| Entire workflow rerun | Setup/build resolves dependencies afresh |
| Setup/build job rerun | Resolve dependencies afresh |
| Downstream test job only rerun | Reuse its existing nightly artifact; no fresh dependency resolution |
| Eligible push without PR context | Use configured baselines |

Do not add `pull_request: edited` as a build trigger.

Preserve applicable existing branch, draft, path, and cancellation policies.

Document that editing the dependency list affects the next setup/build execution, not an already-created environment.

## 6. Reusable action architecture

Proposed responsibilities:

### `setup-dev-nightly`

- Create and initialize the nightly DBT workarea.
- Output its absolute path.
- Expose the nightly identifier when obtainable.

### `resolve-dependency-prs`

- Parse declarations and consult trusted package configuration.
- Resolve PRs/baselines.
- Produce the manifest and summary.
- Keep API credentials scoped to metadata operations.

A Python CLI may implement the logic, with a thin composite action wrapper.

### `setup-dbt-package`

- Check out a resolved SHA under `sourcecode`.
- Perform configured Python installation when required.
- Do not build the entire workarea per package.

### `setup-python-package`

- Check out a resolved SHA under `pythoncode`.
- Perform configured Python installation.

Share checkout implementation between the package actions.

### Existing orchestration action

Retain the existing entry point, or update all callers deliberately.

It should:

1. Initialize the workarea.
2. Resolve selections.
3. Set up packages in the configured order.
4. Build the complete workarea once.
5. Record completion/failure.
6. Prepare the existing nightly artifact.

Source the workarea environment in each shell that needs it. Do not rely on shell state from an earlier action step.

Use explicit action inputs and outputs instead of hidden dependencies on the caller's current directory.

Ensure the checked-out commit matches the manifest before installation.

Use a fresh checkout destination or a controlled update procedure; do not leave stale package content from a previous selection.

## 7. Reporting and manifest

### Actions job summary

Publish the selected combination in the setup/build job summary.

Do not post a PR comment.

Include:

- Resolution timestamp in UTC.
- Main PR identity when applicable.
- Main workflow-selected commit.
- Run ID and run attempt.
- Nightly identifier when available.
- One row per relevant package.

Suggested columns:

| Package | Selection | Source repository | Revision/version |
|---|---|---|---|

Distinguish:

- Main workflow revision.
- Explicit dependency PR override.
- Configured baseline checkout.
- Package supplied by the nightly.

Link PRs and commits where appropriate.

Do not invent a package SHA when the package is supplied as an installed nightly version. Report the available version/provenance instead.

Publish the selection after successful resolution and before installation, so build failures still have a selection record.

Publish actionable resolution errors if resolution fails. Do not label a partial selection as a complete successful manifest.

Escape untrusted text when rendering Markdown or workflow output.

### Machine-readable manifest

Write a versioned JSON manifest, for example:

```json
{
  "schema_version": 1,
  "resolved_at": "<UTC timestamp>",
  "run_id": "<run ID>",
  "run_attempt": "<attempt>",
  "main": {
    "repository": "DUNE-DAQ/drunc",
    "sha": "<workflow-selected SHA>"
  },
  "nightly": {
    "identifier": "<identifier when available>"
  },
  "dependencies": [
    {
      "target_repository": "DUNE-DAQ/druncschema",
      "selection": "pull_request",
      "pull_number": 123,
      "source_repository": "<resolved head repository>",
      "branch": "<resolved head branch>",
      "sha": "<resolved head SHA>"
    }
  ]
}
```

Extend the schema to represent baselines explicitly.

Store it:

- At a known location inside the workarea.
- Inside the nightly artifact.
- As a small diagnostic artifact if needed to retain it when building or packaging fails.

Avoid artifact naming/reuse collisions across rerun attempts. A downstream test must not accidentally consume an environment from another build attempt.

## 8. Security requirements

- Treat PR text and dependency source code as untrusted.
- Validate reference syntax and allowlist target repositories before API requests.
- Use fixed API hosts; do not request arbitrary user-supplied URLs.
- Never interpolate PR descriptions or branch names into executable shell source.
- Use argument arrays or safely quoted environment variables.
- Do not permit PR-controlled paths, install commands, or action references.
- Use minimum required API permissions.
- Do not persist checkout credentials.
- Do not pass resolver credentials to package installation/build.
- Do not introduce privileged `pull_request_target` execution of PR/dependency code.
- Run dependency builds without secrets or write-capable credentials.
- Report unsupported private dependency access clearly; do not add a broad personal token as an implicit workaround.
- Keep trusted registry and workflow behaviour separate from user-supplied dependency data.

## 9. Implementation subtasks

Complete these in order. Each task should be independently testable and avoid unrelated changes.

### Subtask 1 — Define package configuration and reusable setup primitives

**Scope**

- Inspect current nightly setup and package requirements.
- Add trusted package configuration.
- Extract nightly initialization and reusable package setup actions/helpers.
- Share checkout logic.
- Preserve existing caller compatibility.
- Document baseline policy for each initial package.

**Acceptance criteria**

- Python and DBT source placement are independently testable.
- DBT source packages can still request Python installation.
- Environment setup works across separate action steps.
- The complete workarea is built once.
- Any behaviour change from the previous fallback policy is explicit.
- No PR-body parsing is wired into production yet.

### Subtask 2 — Implement the dependency section parser

**Depends on:** agreed input contract; may be developed alongside Subtask 1.

**Scope**

- Implement pure parsing/normalization functions.
- Apply supported-repository and duplicate validation.
- Return structured references and actionable errors.
- Do not perform HTTP requests.

**Acceptance criteria**

Tests cover:

- All supported reference forms.
- Blank lines and bullet variants.
- Missing, single, duplicate, and reversed markers.
- Empty blocks.
- Bare numbers and malformed references.
- Unsupported repositories.
- Case-insensitive duplicates.
- Main-repository overrides.
- Trailing prose and multiple references.
- References outside the block being ignored.
- Invalid hostname and URL path cases.

### Subtask 3 — Implement API resolution and manifest generation

**Depends on:** Subtasks 1–2.

**Scope**

- Fetch the current main PR body.
- Resolve dependency PR metadata.
- Resolve configured baseline checkouts when needed.
- Generate a complete versioned manifest.
- Preserve the workflow-selected main revision.

**Acceptance criteria**

Use mocked API responses to cover:

- Same-repository and fork-based heads.
- Dependency branch names different from the main branch.
- New head SHA returned on a subsequent resolution.
- Closed/merged PRs.
- Inaccessible PRs and authentication failures.
- Missing head repository or SHA.
- Base repository mismatch.
- Bounded transient retries.
- No fallback for explicit dependency failures.
- Push operation without PR context.

### Subtask 4 — Integrate resolution with nightly setup

**Depends on:** Subtasks 1–3.

**Scope**

- Wire resolution and reusable package setup into the orchestration action.
- Check out exact manifest SHAs.
- Remove same-owner/same-branch inference.
- Build and package the resulting environment.
- Make downstream test selection use the same main revision where relevant.

**Acceptance criteria**

- Different dependency forks/branches work.
- Manifest and checked-out commits match.
- Installation/build stops on dependency failure.
- Dependencies are resolved once per setup/build attempt.
- Downstream tests consume the resulting shared environment.
- No test-list discovery silently falls back to another main branch.

### Subtask 5 — Add Actions summary and artifact provenance

**Depends on:** Subtasks 3–4.

**Scope**

- Render the selection summary.
- Include errors when resolution fails.
- Save the manifest inside the nightly artifact.
- Retain diagnostic provenance on build failure as appropriate.
- Handle artifact identity across rerun attempts.

**Acceptance criteria**

- No PR comments are created.
- Selected revisions remain visible when installation/build fails.
- Baselines and overrides are distinguishable.
- Summary rendering escapes untrusted text.
- Manifest content matches the environment consumed by tests.
- Reruns do not select an older artifact accidentally.

### Subtask 6 — Update PR template, lifecycle handling, and documentation

**Depends on:** Subtasks 2–5.

**Scope**

- Add the empty marked dependency block to the template.
- Document accepted references and errors.
- Document baseline behaviour.
- Document refresh and rerun semantics.
- Verify existing eligible triggers without adding description-edit builds.

**Acceptance criteria**

- Template has no active example dependencies.
- Description edits do not trigger builds.
- The next setup/build fetches the current description and dependency heads.
- Test-only reruns reuse their selected artifact.
- Documentation explains how to request a fresh environment.

### Subtask 7 — End-to-end validation

**Depends on:** Subtasks 1–6.

**Scope**

- Add automated contract/integration tests where practical.
- Perform controlled workflow validation.
- Document results and remaining limitations.

**Acceptance criteria**

Demonstrate:

- Empty dependencies use configured baselines.
- Two declared dependencies can resolve from unrelated branches/forks.
- Editing only the description causes no build.
- A later qualifying build uses the edited list.
- Updating a dependency alone causes no main build.
- A subsequent setup/build resolves its new head SHA.
- Malformed input fails before package installation.
- Explicit dependency checkout failure never falls back.
- Summary and manifest identify the tested combination.
- Existing downstream tests and nightly artifact handling still work.

## Definition of done

- [ ] Dependency selection no longer assumes matching branch names.
- [ ] Supported PR references are parsed only inside the marked block.
- [ ] Invalid declarations fail clearly.
- [ ] Package setup is reusable.
- [ ] Main and dependency revision policies are explicit.
- [ ] Dependencies refresh whenever setup/build executes.
- [ ] Description edits do not trigger dependency builds.
- [ ] All test jobs use the selected build's environment.
- [ ] Summary and artifact manifest record provenance.
- [ ] No dependency-reporting PR comments are posted.
- [ ] Tests cover parsing, resolution, integration, and rerun behaviour.
- [ ] Contributor documentation explains the complete workflow.

