# Description
Addresses issue # _Fill this in with the relevant issue number so the relevant issue can be closed._

_This issue should contain the full description of what the PR is intended to deliver._

_Start with a brief description of what this PR changes._

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

- [ ] Pre-commit hooks run successfully if applicable (e.g. `pre-commit run --all-files`)
- [ ] Unit tests pass (`pytest`) - note please use the broadest marker possible
- [ ] Integration tests ran (`daqsystemtest_integtest_bundle.sh`) successfully, or at least minimal system quick test run successfully (`pytest -s minimal_system_quick_test.py`). Note - these are resource intensive and it is recommended that these are not run locally
- [ ] Testing skipped as there are no core code changes in this PR, this only relates to documentation/CI workflows
<!-- - [ ] Drunc integration tests pass (`pytest -m integration_tests`) Note - at the time of creating this template, these tests have not been written hence remain as a TODO. -->

## Final checklist prior to marking this as "Ready for Review"

- [ ] Code is commented where needed, particularly in hard-to-understand areas
- [ ] New unit tests have been added or an issue has been opened to tackle that in the future in : # <issue>

# Developer notes for the reviewer

## Integration testing completed by the developer
- [ ] I have run the minimal system quick test only, please run the rest of the tests
- [ ] I have run the full integration tests bundle, this can be skipped

## Suggested manual testing checklist 

_If applicable, include a description of what should be run to demonstrate and test the changes that are being made._

# Reviewer checklist
_Note - if a reveiwer requests changes and those changes are implemented, this block should be re-checked._

- [ ] Pre-commit hooks run successfully if applicable (e.g. `pre-commit run --all-files`)
- [ ] Unit tests pass (`pytest`) - note please use the broadest marker possible
- [ ] Suggested manual tests pass as described above
- [ ] Integration tests pass (`daqsystemtest_integtest_bundle.sh`)
    - This test takes a long time, it can be left running on its own, and does not have to be monitored as it runs. 
    - The only check that the `drunc` developers should be concerned about in is whether any issues related to `drunc` are mentioned in any of the log files
    - If the issue raised is not related to run control, use the following steps, only move on to the next step if the previous step fails:
      - Validate in a fresh working area
      - Contact the lead developer of the relevant WG (contact Run Control lead if unsure)
      - Notify on the `#daq-release-prep channel` on Slack once another developer has verified this issue
<!-- - [ ] Drunc integration tests pass (`pytest -m drunc_integration_tests`) Note - at the time of creating this template, these tests have not been written hence remain as a TODO. -->