# Description
Fixes issue # _ISSUE NUMBER_

_WHAT DOES THIS PR CHANGE - ONE LINE._

__DON'T FORGET TO LABEL THE ISSUE WITH THE APPROPRIATE TOPIC__

_DOCUMENT THE CHANGE BELOW OR DELETE IT_

The relevant changes in the user workflow have been documented _here_ (link URL)

<!-- _Reminder - the general practice is to discuss plans for large development topics at RC technical meetings prior to developpment, to not waste developer effort. This will be further discussed at CCM/SWIT meetings if relevant._ -->

## Type of change

- [ ] New feature / enhancement
- [ ] Optimization
- [ ] Bug fix
- [ ] Breaking change
- [ ] Documentation

## List of required branches from other repositories
_WHAT PRs NEED TO BE INCLUDED TO MAKE THE CHANGE._

## Change log

_WHAT HAS CHANGED._

## Suggested manual testing checklist 

_LIST COMMANDS TO DEMONSTRATE CHANGE_

<details>
<summary>

# Developer checklist

</summary>

## Prior to marking this as "Ready for Review"

Tests ran on: _WHAT HOSTNAME_ from release _RELEASE_NAME_

Unit tests - some tests can't be ran on the CI. This is [documented](https://github.com/DUNE-DAQ/drunc/wiki/Testing-prior-to-PR-merges). If this PR checks a feature that can't be tested with CI, this has been marked appropriately.

Integration tests - the `daqsystemtest_integtest_bundle` requires a lot of resources, and connections to the EHN1 infrastructure. Check the [cross referenced list](https://github.com/DUNE-DAQ/drunc/wiki#users-with-access-to-clusters-for-running-daqsystemtest_integtest_bundlesh) if you can't run these. The developer needs to run at least the [.](https://github.com/DUNE-DAQ/daqsystemtest/blob/develop/integtest/minimal_system_quick_test.py)

- Unit tests (`pytest --marker`) passed
  - [ ] With relevant marker
  - [ ] Without marker
- Integration tests passed
  - [ ] Only `daqsystemtest_integtest_bundle.sh -k minimal_system_quick_test.py`
  - [ ] Full `daqsystemtest_integtest_bundle.sh`
- [ ] Testing skipped as there are no core code changes in this PR, this only relates to documentation/CI workflows
- [ ] Drunc integration tests pass (`./scripts/drunc_integtest_bundle.sh`)

## Final checklist prior to marking this as "Ready for Review"

- [ ] Code is clearly commented.
- [ ] New unit tests have been added, or is documented in # _ISSUE NUMBER_
- [ ] A suitable reviewer has been chosen from [this list](https://github.com/DUNE-DAQ/drunc/wiki#active-developers).

</details>

<details>
<summary>

# Reviewer checklist

</summary>

- [ ] This branch has been rebased with develop prior to testing.
- [ ] Suggested manual tests show changes.
- [ ] CI workflows fails documented (if present)
- [ ] Integration tests passed
  - Note - if any of the following apply, you can run only `daqsystemtest_integtest_bundle.sh -k minimal_system_quick_test.py`, otherwise run the full `daqsystemtest_integtest_bundle.sh` either on the np0x cluster or on the IC HEP cluster
    - PR changes only affect a few log entries
    - PR changes only affect docstrings
    - PR changes are small, and do not have a large impact on the workflow (use carefully)
  - Only concern yourself if failures related to `drunc` are in the log files
  - If non-`drunc` failure appears:
    - Validate failure in fresh working area
    - Contact Pawel if unsure
- [ ] Drunc integration tests pass (`/scripts/drunc_integtest_bundle.sh`)

Once the above boxes are checked, the PR(s) can be merged.

</details>

<details>
<summary>

# Prior to merging
</summary>
Choose one of the following an complete all substeps

- Changes only affect the Run Control, are in a single repository, and do not affect the end user. 
  - [ ] Changes are documented in docstrings and code comments
  - [ ] Wiki has been updated if architectural or endpoint changes
- Otherwise
  - [ ] Workflow changes demonstrated in the Change Log (if necessary)
  - [ ] Wiki has been updated (if necessary)
  - [ ] #dunedaq-integration Slack channel notified (see below)

Once completed, the reviewer can merge the PR.
<details>

<summary>

## Notification message for a Slack channel
</summary>

Note - this should be to #dunedaq-integration for general workflow that isn't during a release candidate period, and to #daq-release-prep otherwise.

### For an single merge that changes the user workflow
```
The CCM WG has an isolated PR ready to merge that affects user workflows. The PR is:

_URL_

I will leave time for any comments, otherwise will merge these at the end of the work day _Insert your time zone_.
```
### For co-ordinated merge
```
The CCM WG has a set of co-ordinated merges ready to merge. The PRs are:

_URL_

_URL_


I will leave time for any comments, otherwise will merge these at the end of the day.
```

</details>
</details>