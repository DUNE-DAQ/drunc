# Description
Addresses issue # 

_This issue should contain the full description of what the PR is intended to deliver._

## Type of change

- [ ] New feature or enhancement (non-breaking change which adds functionality)
- [ ] Optimization (non-breaking change that improves code/performance)
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] Breaking change (whatever its nature)
- [ ] Documentation (non-breaking change that adds or improves the documentation)

## Requires
If applicable, list all the related PRs required to implement and test this change.

## Change log

_Include a high level overview of the changes introduced by this PR._

## Suggested manual testing checklist 

_If applicable, include a description of what should be run to demonstrate the changes that are being made._

## Testing checklist, complete prior to marking this as "Ready for Review"
Testing was ran on: _include the host name here_

- [ ] Pre-commit hooks run successfully if applicable (e.g. `pre-commit run --all-files`)
- [ ] Unit tests pass (`pytest`)
- [ ] Minimal system quick test passes (`pytest -s minimal_system_quick_test.py`)
- [ ] Drunc integration tests pass (`pytest -m integration_tests`) Note - at the time of creating this template, these tests have not been written hence remain as a TODO.
- [ ] Testing skipped as there are no core code changes in this PR


## Further checks

- [ ] Code is commented where needed, particularly in hard-to-understand areas
- [ ] If applicable, new tests have been added or an issue has been opened to tackle that in the future.
  (Indicate issue here: # (issue))

_The option below is awaiting discussion with SWIT regarding global linting and styling, and separate templates for the different repo types_
- [ ] Code style is correct (`dbt-build --lint`, see [the documentation](https://dune-daq-sw.readthedocs.io/en/latest/packages/styleguide/))

# Reviewer checklist
_Note - if a reveiwer requests changes and those changes are implemented, it is expected that the below block is re-ran._

- [ ] Pre-commit hooks run successfully if applicable (e.g. `pre-commit run --all-files`)
- [ ] Unit tests pass (`pytest`)
- [ ] Suggested manual tests pass
- [ ] Drunc integration tests pass (`pytest -m integration_tests`) Note - at the time of creating this template, these tests have not been written hence remain as a TODO.
- [ ] Minimal system quick test passes (`pytest -s minimal_system_quick_test.py`)
- [ ] Integration tests pass (`daqsystemtest_integtest_bundle.sh`)
    - This test takes a long time, it can be left running on its own, and does not have to be monitored as it runs. 
    - The only check that the `drunc` developers should be concerned about in is whether any issues related to `drunc` are mentioned in any of the log files.
    - If the issue raised is not related to run control, use the following steps, only move on to the next step if the previous step fails:
      - Validate in a fresh working area
      - Contact the lead developer
      - Notify on the `#daq-release-prep channel` on Slack