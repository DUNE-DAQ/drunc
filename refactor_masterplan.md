# Integration-Test Utilities Refactor Master Plan

## Status and Working Agreement

This is a living design document for refactoring `src/drunc/integtest/integ_test_utils.py` and its eventual callers.

- The planning-only scope now permits edits to this Markdown file and the `src/drunc/integtest/proposal.py` skeleton. Do not edit production utilities or integration-test callers until implementation is authorised.
- Build and revise the design here before implementing it.
- Examples are illustrative proposals, not existing APIs or final signatures.
- The eventual implementation should follow the agreed complete plan, rather than implementing each design discussion immediately.
- Preserve the local correctness fixes already made. The focused regression tests and doctests passed, but full DAQ integration scenarios have not been run.

## Goal

Make the utilities easier to read, discover and use by giving related operations clear ownership and a predictable dependency direction.

Classes should represent a cohesive responsibility and useful state. They should not merely collect unrelated static methods under a new name.

## Scope of the Current Design Discussion

Focus on encapsulating output searching and table extraction, while keeping behavioural assertions distinct.

The following topics are deliberately deferred:

- Migrating tests away from raw line manipulation and private parser calls.
- Centralising caller-side ANSI removal and stdout splitting.
- Introducing typed table rows or changing dictionary-based results.
- Replacing name-based controller-role assumptions with explicit role information.
- Relocating environment-dependent pytest policy and filesystem helpers.

The examples below show a possible destination for callers, not authorisation to migrate them now.

## Proposed Responsibilities

### `LogLines`: Search and Marker Selection

Owns a sequence of output lines. Its operations search that same output, rather than requiring callers to pass `lines` to every helper.

Responsibilities:

- Find or require a matching line.
- Find text and regular-expression matches.
- Select exact echo markers.
- Extract output between markers.
- Report missing or ambiguous markers with useful context.

It does not understand table columns, process status or expected FSM states.

For the initial design, input remains an already-prepared `list[str]`. The normalisation boundary, copying policy and source metadata remain later decisions.

| Existing Function | Proposed Method |
| --- | --- |
| `find_line_index` | Internal optional-search operation |
| `require_line_index` | Internal search primitive behind `require(...)` |
| `require_line_containing` | `require(text=..., ...)` |
| `require_pattern_match_index` | `require(pattern=..., ...)` |
| `require_echo_marker_index` | `require(echo=..., ...)` |
| `get_lines_between_markers` | `between(start, end, echo=False, ...)` |
| `get_text_between_echo_markers` | `between(start, end, ...)`, followed by joining the lines when text is needed |

These replace the earlier proposal for several named `require_*` methods. Public result shapes remain provisional because raw-line handling is deferred; see the slimmer search contract below.

Note that the require should also contain a flag or something that also contains the index thing. So an index=...

### `TableReader`: Table Identification and Extraction

Uses `LogLines` through composition. It identifies tables, applies their layouts and parses their rows.

Responsibilities:

- Locate a table within the relevant command output.
- Own supported table titles and column layouts.
- Parse process, status and execution-report tables.
- Parse Run Info into its existing key-value result.
- Validate row widths and table termination.
- Distinguish explicit empty process output from missing or malformed output.
- Query parsed rows and values using the selected table kind's name column.
- Check status-table state/substate expectations through one explicit assertion method.

Proposed public API:

| Method | Result, Keeping Current Shapes |
| --- | --- |
| `after_echo(marker, TableKind.PS)` | `list[dict[str, str]]` |
| `after_echo(marker, TableKind.STATUS)` | `list[dict[str, str]]` |
| `after_echo(marker, TableKind.EXEC_REPORT)` | `list[dict[str, str]]` |
| `after_echo(marker, TableKind.RUN_INFO)` | `dict[str, str]` |
| `after_echo(marker, TableKind.TERMINATED)` | `list[dict[str, str]]` |

This is one extraction method, not five methods. The enum describes the expected listing, including its title and layout; `TERMINATED` reuses the process-row layout but selects termination output. This supersedes the earlier table-specific method proposal. The query and status-assertion methods below are additional public operations.

The current `_get_table_after_echo` and `_parse_table_from_index` become internal implementation details. Title matching and supported column layouts are selected from a private registry keyed by `TableKind`.

### Row Queries and Behavioural Assertions

Fold table-specific row queries and the two status-table assertions into `TableReader`. This supersedes the earlier proposal to keep all row queries and status assertions standalone.

- `rows(table, kind, *, name)` replaces both process-friendly-name and status-name row selectors.
- `value(table, kind, *, name, column)` replaces friendly-name column lookup and works with other supported row-table kinds too.
- `_filter_rows(table, column, value)` is the private filtering primitive, replacing the standalone `_get_rows_from_table` skeleton.
- `assert_status(table, kind, *, state=None, controller_substate=None, non_controller_substate=None)` replaces separate state and substate checks.

All query/check methods operate on an explicitly supplied parsed result, not a mutable "current table" stored in the reader. They neither re-read stdout nor parse it again. Callers pass `kind` because the existing plain list/dictionary results do not carry layout metadata.

The private table specification owns the name column: `friendly_name` for `PS` and `TERMINATED`, and `name` for `STATUS` and `EXEC_REPORT`. `RUN_INFO` is already a key-value dictionary and does not support these row-query methods.

Process presence, table-row UUID validation and execution-report success now also belong to `TableReader` as `assert_presence`, `assert_uuids` and `assert_execution_success`. This is a deliberate expansion into a table-reading/querying/checking facade, not a claim that assertions and parsing are the same operation. Keep their implementations separate internally.

No separate `Assertions` or generic result-wrapper `Table` class is proposed yet. Renaming `TableReader` to `TableHelper` may better describe the expanded responsibility; the name remains open.

### Small Independent Helpers

Move ANSI stripping into private `LogLines._strip_ansi`. This settles its proposed ownership only: when and how callers stop normalising stdout remains deferred. The existing production `strip_ansi` function stays unchanged until implementation is authorised.

Keep `require_pattern_match`, `check_file_containing` and `assert_match_contains_uuid` as standalone skeleton functions while their eventual design is considered. In particular, multiline regex matching must not silently become a line-by-line search.

Filesystem checks and `require_drunc` do not belong inside either reader class. Their eventual home is a separate design decision.

## Dependency Direction

```text
Test code -> TableReader -> LogLines
Test code -> LogLines
Test code -> TableReader row queries / table assertions
Test code -> standalone text / file / UUID-text helpers

TableReader -> parsed lists and dictionaries -> assertions
```

`LogLines` knows nothing about `TableReader`. Table extraction, queries and table assertions share the table facade, but remain distinct operations: extraction reads output, queries select parsed data, and assertions express expectations. Use composition, not inheritance, between readers.

## Illustrative End-State Usage

This example intentionally retains caller-side normalisation using the existing production helper and dictionary results, which are deferred topics. The future normalisation entry point is not yet agreed.

```python
lines = strip_ansi(
    run_dunerc.completed_processes["pmshell"].stdout
).splitlines()

output = LogLines(lines)
tables = TableReader(output)

restart_text = "\n".join(
    output.between("pre_restart_mlt", "post_restart_mlt")
)
require_pattern_match(
    restart_text,
    restart_pattern,
    error_message="Expected the restart log message.",
)

processes = tables.after_echo("post_restart_mlt", TableKind.PS)
tables.assert_presence(processes, TableKind.PS, name="mlt", context="after restart")

status = tables.after_echo("test_conf_done", TableKind.STATUS)
tables.assert_status(status, TableKind.STATUS, state="configured")
```

The example operations need not all occur in the same integration scenario. The intended benefit is discoverability: `output` searches and checks logs; `tables` extracts, queries and checks tables; the few remaining standalone helpers are explicitly undecided.

## Initial File Organisation

Keep the design in the existing utility module initially; splitting files is not required to establish ownership.

1. Module overview, imports and independent constants/helpers.
2. `LogLines`.
3. Table layouts and `TableReader`.
4. Row queries and table assertions within `TableReader`, with private filtering alongside its other helpers.
5. Remaining standalone text and UUID-text helpers pending their design decision.
6. Clearly separated pytest/environment and filesystem utilities pending their own design discussion.

Table layouts should remain together near their owning reader. Avoid a generic parser framework, factories or inheritance hierarchies unless a concrete requirement justifies them.

## Design-Building Order

These are discussion milestones, not instructions to edit Python files.

1. Agree the responsibilities and public methods of `TableReader` first. Table extraction is already a cohesive group.
2. Agree the `LogLines` interface and how `TableReader` consumes it.
3. Resolve table boundaries, repeated-marker selection and terminated-table access.
4. Revisit the deferred topics one at a time.
5. Finalise caller migration, compatibility and verification before authorising implementation.

Temporary function wrappers are an option if compatibility requires a staged migration. They are not assumed necessary for an eventual coordinated implementation.

## Behaviour to Preserve

- Exact marker identity; missing or ambiguous echo markers must not silently select unrelated output.
- Table searches must not borrow output from later commands.
- Known six- and seven-column short process layouts remain supported.
- Malformed rows and truncated tables fail explicitly.
- Only recognised no-process output produces an empty process listing.
- Failure messages retain enough context to diagnose extraction problems.
- Existing dictionary keys and assertion semantics remain unchanged unless a later decision explicitly changes them.

## Open Decisions

- Are `LogLines` and `TableReader` the clearest names?
- Should table methods use the next echo as their boundary, accept an explicit end marker, or support both?
- How should callers select repeated marker occurrences intentionally?
- Should `TableKind.TERMINATED` be the name for terminated-process listings, or should the enum distinguish layout from output title differently?
- Are explicit `text`, `pattern` and `echo` keyword selectors the preferred unified search syntax?
- Should table specifications remain private constants or become small immutable values?
- Should failures continue to use `AssertionError`, or should extraction errors gain a dedicated exception type later?
- Which existing function names must remain available during migration?
- When, if ever, should the module be split into separate files?

## Current Function Usage Inventory

Snapshot: 2026-10-12. A static AST scan counted imported utility calls in the integration-test modules, calls within the utility module itself, and calls in the helper regression tests separately. An accompanying workspace import search found 13 integration-test consumers and one unit-test consumer.

These are source-level call sites, not execution counts. Parametrisation and loops may execute one call site many times. Docstring examples are excluded. This inventory does not establish usage in external repositories or through dynamic imports.

Caller abbreviations used below:

- `C`: `controller_test.py`.
- `P`: `pm_pms_test.py`.
- `U`: `pm_us_test.py`.
- `A`: `pmaas_test.py`.
- `M`: `process_manager_test.py`.
- `B`: both `failure_mode_death_on_boot_{nest,top}_app_test.py` modules.
- `D`: both `failure_mode_death_post_boot_{nest,top}_app_test.py` modules.
- `F`: both `failure_mode_fsm_cmd_death_{nest,top}_app_test.py` modules.
- `T`: both `failure_mode_fsm_cmd_timeout_{nest,top}_app_test.py` modules.
- `All failure modes`: the eight modules represented by `B`, `D`, `F` and `T`.

### Functions Directly Called by Integration Tests

| Function | Direct Call Sites | Caller Files | Callers |
| --- | ---: | ---: | --- |
| `strip_ansi` | 79 | 13 | C, P, U, A, M, all failure modes |
| `find_line_index` | 4 | 3 | P, U, A |
| `check_file_containing` | 6 | 6 | B, D, T |
| `require_line_containing` | 15 | 4 | P, U, A, M |
| `require_echo_marker_index` | 14 | 4 | P, U, A, M |
| `require_pattern_match_index` | 10 | 3 | P, U, M |
| `require_pattern_match` | 19 | 3 | P, U, M |
| `get_lines_between_markers` | 2 | 2 | P, U |
| `assert_contains_between_markers` | 3 | 2 | P, U |
| `get_text_between_echo_markers` | 6 | 2 | P, U |
| `assert_match_contains_uuid` | 4 | 2 | P, U |
| `assert_rows_have_valid_uuids` | 3 | 3 | P, U, A |
| `_parse_table_from_index` | 4 | 3 | P, U, A |
| `get_ps_table_after_echo` | 39 | 12 | P, U, A, M, all failure modes |
| `get_status_table_after_echo` | 18 | 10 | C, U, all failure modes |
| `get_execution_report_after_echo` | 2 | 2 | C, U |
| `get_column_for_friendly_name` | 5 | 3 | P, U, M |
| `get_rows_by_friendly_name_from_ps_table` | 6 | 6 | B, D, F |
| `get_rows_by_name_from_status_table` | 18 | 8 | All failure modes |
| `assert_process_presence` | 15 | 3 | P, U, M |
| `check_execution_report_success` | 2 | 2 | C, U |
| `check_status_table_states` | 2 | 2 | C, U |
| `check_status_table_substates` | 2 | 2 | C, U |
| `get_run_info_after_echo` | 2 | 2 | C, U |

Low call-site counts do not imply low value: the execution-report and FSM assertions sit in shared parametrised test helpers.

### Functions Not Directly Called by Integration Tests

| Function | Internal Call Sites | Other Observed Usage | Proposed Classification |
| --- | ---: | --- | --- |
| `require_line_index` | 4 | None outside the module | Internal search primitive |
| `_matches_marker` | 3 | None outside the module | Internal marker matcher |
| `get_table_columns` | 1 | One call site in helper unit tests | Internal schema lookup |
| `_get_table_after_echo` | 4 | None outside the module | Internal table extraction |
| `get_rows_from_table` | 2 | None outside the module | Internal primitive behind named row queries |

All five have internal callers. They are candidates for hiding, not deletion. There are no unused explicit imports of this utility module's names in the scanned integration-test consumers.

### Non-Function API and Unit-Test Usage

`require_drunc` is a pytest marker, not a function. All eight failure-mode modules use it; retain it as a deliberately supported export until environment policy is redesigned.

The helper regression tests additionally call `_parse_table_from_index` at five source locations and `get_table_columns` at one. They also exercise the public marker and table helpers. Unit-test access does not make an implementation detail part of the supported integration-test API.

When migrating those tests, use explicit expected column keys rather than constructing expected results with the same schema getter being tested. Otherwise a wrong schema can agree with itself.

## Proposed Public and Internal API Boundary

Public means supported for integration-test authors, documented with examples and stable contracts. Internal means free to change with the implementation; integration tests should not import or call it.

### Supported Surface for Test Authors

| Area | Proposed Public Surface | Existing Needs Covered |
| --- | --- | --- |
| Log searching/checking | `LogLines.require(...)`, `between(...)` and `assert_contains_between(...)` | Text/pattern/echo searches, wait timing, restart output and marker-window text assertions |
| Table reading | `TableReader.after_echo(marker, kind)` with `TableKind` | All four existing table result shapes |
| Termination output | `TableReader.after_echo(marker, TableKind.TERMINATED)` | The four current private-parser calls in P, U and A |
| Row queries | `TableReader.rows(table, kind, name=...)` and `value(table, kind, name=..., column=...)` | Existing process/status row selection and column lookup |
| Status assertions | `TableReader.assert_status(table, TableKind.STATUS, ...)` | Existing state and controller/non-controller substate checks |
| Other table assertions | `TableReader.assert_presence(...)`, `assert_uuids(...)` and `assert_execution_success(...)` | Name presence, UUID cells and execution-report expectations |
| Independent helpers | `require_pattern_match` and `assert_match_contains_uuid`, unchanged pending a decision | Existing text-level regex and captured-UUID assertions |
| File and environment checks | `check_file_containing` and `require_drunc`, kept separate from readers | Existing failure-mode tests |

The generic table method and enum selectors are proposals, not agreed signatures. Their boundary-selection options still need to be resolved.

The current public `LogLines` index-returning methods remain provisional because raw-line handling is deferred. In particular, do not make a generic predicate search public solely because `find_line_index` is currently used: all four observed uses locate terminated-process tables and could disappear behind the proposed table method.

### Implementation Details

- Generic required-line searching and marker matching belong inside `LogLines`, with leading-underscore method names where they are not part of the supported API.
- Table parsing, title matching, supported widths and schema lookup belong inside or alongside `TableReader` as private implementation details.
- Generic dictionary-row filtering becomes `TableReader._filter_rows`, behind the public kind-aware `rows` and `value` methods.
- The utility module should not expose a public method that accepts arbitrary table offsets merely to preserve the current private-parser workaround.

### How to Make the Boundary Visible

1. Define an explicit module `__all__` containing the supported classes, standalone functions and pytest marker. This is documentation and export guidance, not access enforcement.
2. Use leading underscores consistently for internal functions, methods and constants. Non-underscored names must have an intentional public role.
3. Put the supported API and its usage examples at the top of the module documentation; do not present the internal call chain as a menu for test authors.
4. Distinguish public behavioural tests from any deliberately white-box parser tests.
5. Once caller migration is authorised, add a lightweight check that integration tests do not import or call private utility members. No new lint framework is required just for this rule.
6. Document any temporary compatibility wrappers as transitional, with an explicit removal condition rather than leaving two competing APIs indefinitely.

## Evidence-Based Reduction Opportunities

These are proposals for the eventual implementation, not permission to change Python files now.

| Opportunity | Evidence | Expected Reduction |
| --- | --- | --- |
| Hide generic search primitives | `require_line_index` has only internal callers | Smaller supported search API; retain the implementation primitive |
| Hide schema and row-filter primitives | `get_table_columns` and `get_rows_from_table` have no integration callers | Fewer public-looking names without removing useful internal reuse |
| Replace offset-based termination parsing | All four `find_line_index` calls locate termination tables; the same three modules call `_parse_table_from_index` | The generic reader with `TableKind.TERMINATED` replaces a search-plus-private-parser workflow |
| Encapsulate common table extraction | Four public table helpers already delegate to `_get_table_after_echo` | One public reader method with an enum replaces the table-specific entry points |
| Fold row queries into the table facade | Process and status row selection differ mainly in their name-column key | Two generic query methods replace three table-specific standalone queries |
| Combine status-table checks | State and substate assertions both validate rows of the same layout | One method supports state-only, substate-only or combined expectations |
| Reuse named UUID assertions consistently | A shared row-UUID assertion exists, while several tests still contain direct UUID loops | Potentially less repeated assertion code; audit those loops during caller migration |
| Avoid premature deletion of convenience helpers | Every currently non-internal function has actual integration callers, except the two public-looking internal primitives | Preserve readable test vocabulary instead of reducing names merely for a lower count |

The immediate design target is a smaller, clearer supported surface, not the fewest possible functions. Encapsulation can improve discoverability while retaining the same number of useful implementation helpers.

## Slimmer API Proposal

The usage inventory counts call sites, not redundant work. Combining entry points reduces the number of names test authors must learn; it does not necessarily reduce how often tests need to search or parse output.

### One Table Entry Point

Use `TableReader.after_echo(marker, kind, ...)`, conceptually equivalent to `get_table_after_echo(lines, marker, kind)` if a function-based API were retained.

```python
processes = tables.after_echo("post_boot", TableKind.PS)
status = tables.after_echo("post_boot", TableKind.STATUS)
report = tables.after_echo("test_conf", TableKind.EXEC_REPORT)
run_info = tables.after_echo("test_start_done", TableKind.RUN_INFO)
terminated = tables.after_echo("test_terminate", TableKind.TERMINATED)
```

Use `TableKind.STATUS` when replacing `get_status_table_after_echo`; `TableKind.PS` selects a different table.

- The enum selects a supported output format; callers do not supply arbitrary title regexes, columns or offsets.
- A private registry holds title rules, layouts, permitted widths and explicit empty-listing rules.
- `RUN_INFO` retains its dictionary result; the other kinds retain lists of row dictionaries. A single method therefore has a kind-dependent result shape. Document it clearly; overloads can be considered when typing is revisited.
- `TERMINATED` shares process columns but has its own title rule. Do not add a second public parser merely for that title.
- Preserve bounded extraction and fail when the expected kind is missing. Do not infer another table kind from whatever happens to appear.
- Reject unsupported kind values rather than falling back to a default layout.

No permanent `processes_after`, `status_after` or similar aliases are proposed. Their readability is now provided by the enum member at the call site.

### One Required Search, Explicit Matching Modes

Instead of separate public `require_line_containing`, `require_pattern_match_index` and `require_echo_marker_index` functions, use one method with mutually exclusive keyword selectors:

```python
header_idx, _ = output.require(text="root-controller logs")
wait_marker_idx, _ = output.require(echo="test_wait")
running_idx, running_match = output.require(pattern=running_pattern, start_idx=wait_marker_idx + 1)
```

The wait-marker lookup bounds the subsequent running-pattern search. An optional `error_message` can add scenario-specific context.

Provisional contract, retaining existing index-based workflows rather than introducing a new search-result class:

- Return `(line_index, regex_match_or_none)` consistently. Only the `pattern` selector produces a regex match.
- `text=` means substring containment in one line.
- `pattern=` accepts a compiled regex and searches one line at a time, returning captured groups through the match.
- `echo=` means an exact marker in the `drunc.echo` logger's output, not a generic substring search.
- Require exactly one selector. Zero or multiple selectors are invalid API usage and raise `ValueError`.
- Required output not found raises an assertion with the selector and search context, plus any caller-supplied message.
- Text and pattern searches retain first-match behaviour; echo selection retains ambiguity detection. Unification must not erase those different contracts.
- Retain `start_idx` for existing workflows. Explicit occurrence selection remains an open design decision.

Alternative syntax is `require(query, kind=MatchKind.REGEX)`. Prefer keyword selectors for now: they avoid a second public enum and make it clear whether a string is literal text or an echo marker. Do not silently interpret strings as regexes.

An optional-search primitive remains private. Current integration uses of `find_line_index` are all termination-table searches, which the table reader can own. Add a public `find(...)` only if a genuine optional-search use case remains after migration.

### One Marker-Window Operation

Use `between(start, end, *, echo=True, ...) -> list[str]` for marker-delimited output:

```python
restart_lines = output.between("pre_restart_mlt", "post_restart_mlt")
restart_text = "\n".join(restart_lines)
manager_lines = manager_output.between("pre_boot", "post_boot", echo=False)
```

- The default recognises exact `drunc.echo` markers.
- `echo=False` recognises complete marker tokens at the end of log lines, for manager-side markers with a different logger.
- Both modes require an ordered start/end pair and exclude marker lines from the result.
- Always return lines. Joining them when text is needed avoids separate public text-returning and line-returning helpers or a `return_text` flag.
- `LogLines.assert_contains_between(start, end, expected_text, *, echo=False, start_idx=0)` owns the existing marker-window text assertion. It uses `between` internally and retains the marker names and selected output in its failure message.
- Unlike `between`, the assertion defaults to `echo=False` to preserve the original helper's generic end-of-line marker semantics. Use `echo=True` explicitly when logger-aware echo selection is intended.

### Do Not Merge Different Search Domains Accidentally

`require_pattern_match(text, pattern)` searches an already-extracted string and may match across several lines. That is not the same operation as finding one matching log line and its index.

Keep that small standalone helper for now, potentially sharing a private regex-checking primitive. Do not force text-level matching into `LogLines.require` using an `across_lines` flag merely to remove one function name.

UUID-text checks remain standalone while their design is considered. Table-row UUID checks, process presence and execution-report checks belong to `TableReader`, not `LogLines`.

### Kind-Aware Row Queries and Status Checks

The six functions requested for consolidation map to three public methods and one private primitive:

| Existing Function | Proposed Owner/Operation |
| --- | --- |
| `get_column_for_friendly_name` | `TableReader.value(table, kind, name=..., column=...)` |
| `get_rows_by_friendly_name_from_ps_table` | `TableReader.rows(table, TableKind.PS, name=...)` |
| `get_rows_by_name_from_status_table` | `TableReader.rows(table, TableKind.STATUS, name=...)` |
| `_get_rows_from_table` | Private `TableReader._filter_rows(...)` |
| `check_status_table_states` | `TableReader.assert_status(..., state=...)` |
| `check_status_table_substates` | `TableReader.assert_status(..., controller_substate=..., non_controller_substate=...)` |

```python
processes = tables.after_echo("after_crash", TableKind.PS)
mlt_rows = tables.rows(processes, TableKind.PS, name="mlt")
mlt_status = tables.value(processes, TableKind.PS, name="mlt", column="status")

status = tables.after_echo("test_conf_done", TableKind.STATUS)
root_rows = tables.rows(status, TableKind.STATUS, name="root-controller")
tables.assert_status(status, TableKind.STATUS, state="configured")
```

For a combined check, pass `state="configured"`, `controller_substate="configured"` and `non_controller_substate="idle"` to the same `assert_status` call.

Proposed contracts:

- `rows` returns all exact name matches after stripping cell whitespace; no match returns `[]`.
- `value` requires a matching row and returns its requested column. Retain the existing first-matching-row behaviour initially; deciding how to handle ambiguous process names across sessions is a separate follow-up.
- Validate that `kind` supports row queries and that the requested column belongs to its layout. A missing optional cell, such as an absent exit-code column, must fail explicitly when requested.
- Do not pretend that `TableKind` proves a supplied table's origin. Validate the required shape; plain dictionaries still lack provenance until a later result-model decision.
- `assert_status` accepts only `TableKind.STATUS`. Passing `PS` or another kind is invalid usage, not permission to treat process liveness as an FSM state.
- State-only checks and substate-only checks remain possible. If either substate expectation is supplied, require both controller and non-controller values; do not introduce a silent default.
- Require at least one expectation, reject empty status tables, and include row names and actual values in failures.
- Preserve the current name-based controller/non-controller distinction for now; explicit roles remain deferred.
- Keep optional expectations independent so a combined check validates both state and substates rather than choosing only one.

Only the planning skeleton changes at this stage. Production helpers and their callers remain untouched until implementation is authorised.

### Remaining Helpers: Clear Ownership Moves

The cross-check against the originals gives these immediate skeleton moves:

| Original Helper | Proposed Home | Contract to Preserve |
| --- | --- | --- |
| `strip_ansi` | Private `LogLines._strip_ansi(text)` | Remove styling without changing log content; caller normalisation policy remains deferred |
| `assert_contains_between_markers` | `LogLines.assert_contains_between(...)` | Require markers, exclude marker lines, search substrings and report the selected output on failure |
| `assert_rows_have_valid_uuids` | `TableReader.assert_uuids(table, kind)` | Validate every UUID cell with row-name context |
| `assert_process_presence` | `TableReader.assert_presence(table, kind, name=..., present=..., context=...)` | Assert exact-name presence or absence, preserving diagnostic context |
| `check_execution_report_success` | `TableReader.assert_execution_success(table, kind)` | Require nonempty output and success in both execution columns |

For table assertions:

- `assert_uuids` uses the kind's UUID and name columns. The currently supported UUID-bearing layouts are `PS` and `TERMINATED`; other kinds are invalid usage. Arbitrary custom-column compatibility can be revisited if required during migration.
- `assert_presence` uses the existing kind-aware row query and supports row-table kinds with a name column. It does not support the Run Info dictionary.
- Absence checks may accept an explicit empty process listing, but missing/unparseable command output must already have failed extraction. Plain lists do not prove provenance; a later typed-result design may address that limitation.
- `assert_execution_success` accepts only `EXEC_REPORT`, rejects empty reports and checks both `command_execution` and `fsm_transition` using the original expected messages.
- Queries and assertions consume supplied parsed tables; they do not change a hidden active table or reparse stdout.

Leave these skeleton functions untouched while deciding their future:

- `require_pattern_match`: arbitrary string/block regex matching, including multiline captures.
- `check_file_containing`: filesystem access and checking multiple substrings, with its current boolean/missing-file behaviour.
- `assert_match_contains_uuid`: regex extraction followed by validation of capture group 1; it returns the captured UUID.

Do not fold file access or table assertions into `LogLines` merely to remove the remaining standalone names. The pytest marker `require_drunc` remains a separate non-function export, outside the skeleton's function/class outline.

### Proposed Public Reader Surface

| Owner | Public Entry Points | Internal Responsibilities |
| --- | --- | --- |
| `TableReader` | `after_echo(...)`, `rows(...)`, `value(...)`, `assert_status(...)`, `assert_uuids(...)`, `assert_presence(...)`, `assert_execution_success(...)` | Bounded parsing, layout-aware queries and table assertions |
| `LogLines` | `require(...)`, `between(...)`, `assert_contains_between(...)` | Private ANSI stripping/scanning, selector validation, marker matching and window assertions |

This gives ten public reader/facade operations: seven on `TableReader` and three on `LogLines`. Constructors, `TableKind` and the three undecided standalone helpers are additional API elements. These ownership moves reduce loose module-level functions, not the number of distinct behaviours that need testing.

## Verification for Eventual Implementation

- Preserve and adapt the existing focused output-extraction regression tests.
- Exercise each supported table type with representative Rich-rendered output.
- Cover missing/ambiguous markers, command boundaries, invalid widths, truncated tables and explicit empty listings.
- Check every table enum member, including termination output and the distinct Run Info result shape.
- Check all unified search selectors, invalid selector combinations, capture groups and matching/ambiguity policies.
- Check kind-aware name selection, missing rows/columns, unsupported Run Info queries, and state-only/substate-only/combined status assertions.
- Check marker-window text assertions in both logger-aware and generic-marker modes, and table UUID/presence/execution assertions with valid, invalid and empty inputs.
- Test public reader methods, rather than depending primarily on private parsing methods.
- Validate migrated integration scenarios as well as helper unit tests.
- Source `. ./env.sh` from the NFD_DEV workspace root before running Python checks.

## Decision Log

| Status | Decision |
| --- | --- |
| Agreed | Planning edits may update this Markdown file and the proposal skeleton; production implementation remains unauthorised. |
| Agreed | Develop the master plan before authorising implementation. |
| Agreed | Caller raw-line handling, caller normalisation and typed tables are deferred. |
| Proposed | Introduce `TableReader` and `LogLines` with one-way composition. |
| Proposed | Fold row queries and table-specific assertions into `TableReader`; leave undecided text/file helpers standalone. |
| Proposed | Establish ownership within the existing module before considering file splits. |
| Recorded | Usage inventory distinguishes integration calls, internal calls and helper unit-test usage. |
| Proposed | Declare supported exports explicitly and keep generic search, schema lookup and table parsing internal. |
| Proposed | Replace table-specific reader methods with `after_echo(marker, TableKind...)`, including terminated-process output. |
| Proposed | Consolidate required line searches into `require(...)` with explicit text/pattern/echo selectors. |
| Proposed | Consolidate marker-window extraction into `between(...)`; keep multiline string-regex matching separate. |
| Proposed | Use kind-aware `rows`/`value` methods and a single status assertion instead of table-specific standalone helpers. |
| Agreed | Move obvious log/table helpers into their owning classes in the skeleton only; leave the remaining helpers under consideration. |
| Proposed | Keep ANSI stripping private to `LogLines` without yet changing caller-side normalisation. |
