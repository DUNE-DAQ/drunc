> Sample uses of this template can be found in DUNE-DAQ/daq-deliverables#181 and DUNE-DAQ/daq-deliverables#187.

**_Small Issues and PRs_**

> _Small_ changes are ones that have little or no impact on users or other parts of the TDAQ system.  Please provide the information requested in this section for *all* Issues and PRs (small, medium, or large).

> Please provide a Description and Testing Suggestions for all Issues and PRs, as shown below.  Also, please set values for the Status, Impact Radius, and Working Group fields in the Tracking Project list.

> If the Target Release is known, please set its value in the Tracking Project list and set the Status to _Assigned to Release_.  If not, then the Status can be set to _Todo_.

> If an Issue is a sub-issue, please link it to the parent Issue using the GitHub functionality for doing that.  If a PR is part of a parent Issue, please specify a reference to the parent Issue in the _Parent Issue (for PRs)_ field using a format of DUNE-DAQ/\<repo-name\>#\<issue number\>.

## Description

 > What is changing and why.  For example, what prompted this change?

## Testing Suggestions

> For example, what steps would a reviewer use to see the change in action?

> For _medium_ and _large_ Issues, it may be reasonable to simply say "Please see the tests that are described in the child Issues."

**_Medium Issues_**

> Please provide the requested information in this section for all medium and large Issues.

> _Medium_ changes are ones that A) affect multiple repositories or include multiple Issues or PRs in a single repo or B) will benefit from coordination between developers.  In the multi-repo/Issue/PR case, a parent Issue that is identified as being of Medium size will be helpful in communicating the full set of changes that are part of a deliverable.  In cases that will benefit from coordination between developers, the information that is requested in this section will be used to inform developers about when changes are coming or changes that they will need to make in response to the Medium change set.

> Please provide information about Correlated Issues/PRs and Useful Coordination for all medium and large Issues, as shown below.  (In addition to the information described above...)

> Once a Target Release is known for medium and large Issues, please set the Target Date field in the Tracking Project list.

## Correlated Issues and/or PRs

> If multiple Issues or PRs are part of this change, list them here.  The sub-issue functionality in GitHub can also be used.

## Useful Coordination

> List whatever coordination between developers or groups will be useful, such as the timing of merges or updating of documentation or updating of configurations

> An example: No special coordination will be needed for merging these changes to develop branches beyond announcing the merges on the appropriate Slack channel.

**_Large Issues_**

> _Large_ changes  are ones that have non-trivial impact on users, developers, or testers.  This can be changes in software or user interfaces, changes in the way that users run the system, or changes in underlying system behavior.  It could also be changes in existing test procedures (e.g. regression tests) or new tests that that need to be developed.

> Please provide information regarding the Impact on Developers or Users, Changes in System Behavior, and Needed Changes in Testing for all large Issues, as shown below.  (In addition to the information described above...)

## Impact on Developers or Users

> For example, API changes or new run control commands

## Changes in System Behavior

> For example, changes in how data is drained from the system when a run is stopped

## Needed Changes in Testing

> For example, new regression tests that are needed, or instructions for special tests that should be run at EHN1
