# Description
Fixes issue # _Fill this in with the relevant issue number so the relevant issue can be closed._

_Start with a brief description of what this PR changes._

_If the user workflow has changed, otherwise delete the following line_

The relevant changes in the user workflow have been documented _here_ (link URL)

<!-- _Reminder - the general practice is to discuss plans for large development topics at RC technical meetings prior to developpment, to not waste developer effort. This will be further discussed at CCM/SWIT meetings if relevant._ -->

## Type of change

- [ ] New feature or enhancement (non-breaking change which adds functionality)
- [ ] Optimization (non-breaking change that improves code/performance)
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] Breaking change (whatever its nature)
- [ ] Documentation (non-breaking change that adds or improves the documentation)

## List of required branches from other repositories
_If applicable, list all the related PRs required to implement and test this change._

## Change log

_Include a high level overview of the changes introduced by this PR._

## Initial checklist prior to marking this as "Ready for Review"
Tests ran on: _include the host name here_

Unit tests - for some stages of development, additional deeper tests have been written to test functionality that cannot be ran as part of a CI. These tests have been separated out with [run control specific pytest markers](https://github.com/DUNE-DAQ/drunc/wiki/Testing-prior-to-PR-merges). If the development in this PR is relevant to the scope of these pytest markers, run them as is documented in the link. Otherwise the CI workflow will run the base (unmarked) tests.

Integration tests - the `daqsystemtest_integtest_bundle` will need to be run on a server with large resources and connections to the EHN1 infrastructure. Check the [cross referenced list](https://github.com/DUNE-DAQ/drunc/wiki#users-with-access-to-clusters-for-running-daqsystemtest_integtest_bundlesh) of developers who have access to clusters where the full testing bundle can be ran. The developer needs to run at least the 

Note - Most institutional clusters should be able to run these tests. See the [notes on running when away from CERN or FNAL](https://github.com/DUNE-DAQ/drunc/wiki/Testing-when-not-at-cern-fnal).

- Unit tests (`pytest --marker`) passed
  - [ ] Ran with relevant marker
  - [ ] Skipped as no marker relevant
- Integration tests (`daqsystemtest_integtest_bundle.sh`) passed
  - [ ] Ran `daqsystemtest_integtest_bundle.sh -k minimal_system_quick_test.py` only
  - [ ] Ran `daqsystemtest_integtest_bundle.sh` fully
- [ ] Testing skipped as there are no core code changes in this PR, this only relates to documentation/CI workflows
<!-- - [ ] Drunc integration tests pass (`pytest -m integration_tests`) Note - at the time of creating this template, these tests have not been written hence remain as a TODO. -->

## Final checklist prior to marking this as "Ready for Review"

- [ ] Code changes are commented where needed, particularly in hard-to-understand areas
- [ ] New unit tests have been added or an issue has been opened to tackle that in the future in : # <issue>

# Developer notes for the reviewer
Please select and assign a reviewer from the group this PR relates the most to from list of primary developers [listed in the wiki](https://github.com/DUNE-DAQ/drunc/wiki#active-developers).

## Suggested manual testing checklist 

_If applicable, include a description of what should be executed to demonstrate and test the features, bug fixes, or other changes made within this PR._

# Reviewer checklist
_Note - if a reveiwer requests changes and those changes are implemented, this block should be re-checked._

- [ ] Suggested manual tests pass as described above
- [ ] All CI workflows passed/failures documented
- [ ] Integration tests passed (`daqsystemtest_integtest_bundle.sh. --stop-on-failure`)
    - This test takes a long time, it can be left running on its own, and does not have to be monitored as it runs. 
    - The only check that the `drunc` developers should be concerned about in is whether any issues related to `drunc` are mentioned in any of the log files
    - If the issue raised is not related to run control, use the following steps, only move on to the next step if the previous step fails:
      - Validate in a fresh working area
      - Contact the lead developer of the relevant WG (contact Run Control lead if unsure)
      - Notify on the `#daq-release-prep channel` on Slack once another developer has verified this issue
<!-- - [ ] Drunc integration tests pass (`pytest -m drunc_integration_tests`) Note - at the time of creating this template, these tests have not been written hence remain as a TODO. This will test all the Process Managers in a test bundle. -->

Once the features are validated and both the unit and integration tests pass, the PRs is ready to be merged.

# Prior to merging
## For documentation only PRs

- [ ] All CI workflows have passed

## For all other PR types
Choose one of the following an complete all substeps

- These changes are internal to the Run Control and in a single repository, and do not affect the end user. 
  - [ ] The relevant changes have been thoroughly documented in docstrings and code comments
  - [ ] The wiki has been updated if it is relevant to do so (architectural or endpoint changes).
- The changes affect the workflow of end users, or span multiple repositories
  - [ ] Workflow changes have been demonstrated in the Change Log 
  - [ ] The relevant sections have been documented here in the wiki (link is in the Description section)
  - [ ] A message has been posted to the #daq-sw-librarians Slack channel in the DUNE workspace to announce this to the broader developer community.

Once the requested reviewer is satisfied with the PR, the PR can be merged.

## Notification message for #daq-sw-librarians Slack channel

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