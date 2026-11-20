# Description

Closes #_ISSUE NUMBER_

_WHAT DOES THIS PR CHANGE - ONE LINE._


_DOCUMENT THE CHANGE BELOW OR DELETE IT_

The relevant changes in the user workflow have been documented _here_ (link URL)

<!-- _Reminder - the general practice is to discuss plans for large development topics at RC technical meetings prior to developpment, to not waste developer effort. This will be further discussed at CCM/SWIT meetings if relevant._ -->

## Type of change

___DELETE THOSE THAT DONT APPLY___

- New feature / enhancement
- Optimization
- Bug fix
- Breaking change
- Documentation

## Dependency PRs for CI

List dependency PRs that must be included when building the test workarea.
Use one PR reference per bullet, either `owner/repository#123` or a full GitHub PR URL.
Currently supported dependency repositories are `DUNE-DAQ/druncschema` and
`DUNE-DAQ/daqpytools`. `drunc` always uses the revision selected by the workflow.
Unlisted packages use their nightly-provided versions. Leave this block empty
when no dependency override is needed.

<!-- dependency-prs:start:v1 -->
<!-- dependency-prs:end:v1 -->

Editing this section does not start a build. The next eligible setup reads the
current PR description. To rerun the full integration-test workflow and refresh
dependency selections, comment `/rerun-integration-tests` on this PR. A qualifying
`testing.yml` run must already exist for the current PR revision.

## Change log

_WHAT HAS CHANGED._

## Suggested manual testing checklist 

_LIST COMMANDS TO DEMONSTRATE CHANGE_


## Checklist prior to "Ready for Review"

_CHOOSE ONE AND COMPLETE ALL STEPS_

<details>

<summary>
Documentation only
</summary>

- [ ] Testing skipped as there are no core code changes in this PR, this only relates to documentation/CI workflows

</details>


<details>

<summary>
Code changes
</summary>

Delete inside bullets which doesn't appl, and fill in those in bold italics

- [ ] Tests ran on: **_WHAT HOSTNAME_** from release **_RELEASE_NAME_**
- [ ] Unit test passed:
  - With relevant marker **_INSERT MAKER NAME HERE_**
  - Relying on the CI workflow
- [ ] Integration test passed
  - Only `daqsystemtest_integtest_bundle.sh -k minimal_system_quick_test.py`
  - Full `daqsystemtest_integtest_bundle.sh`
  - Drunc integration test `dunedaq_integtest_bundle.sh -r drunc`
- [ ] Code is clearly commented.
- [ ] New unit tests have been added, or is documented in # _ISSUE NUMBER_

</details>


