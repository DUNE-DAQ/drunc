# Make controller discovery safe for Run Control RPC workers

## Context

The unified shell and process-manager shell call
`ProcessManagerDriver.boot()` on their main thread. After launching
applications, the driver discovers the root controller.

During discovery, the driver temporarily installs a SIGINT handler.
Ctrl+C interrupts the lookup and prints manual connection instructions;
it does not terminate the launched applications.

Run Control calls the same driver from a gRPC worker thread. Python
only permits signal-handler registration on the main thread, so boot
fails with:

> ValueError: signal only works in main thread of the main interpreter

## Immediate workaround

Only install and restore the SIGINT handler when discovery runs on the
main thread.

This preserves existing shell behaviour and avoids the worker-thread
exception. It does not provide remote cancellation.

## Existing timeout behaviour

Connectivity-service discovery uses a timeout of:

    60 seconds × segment-tree depth + 60 seconds

The root counts as depth 1, giving a minimum of 120 seconds.

If lookup times out, the driver logs troubleshooting instructions and
attempts to obtain an address from static OKS configuration. This differs
from SIGINT, which interrupts discovery without attempting that fallback.

The timeout is checked between lookups. Individual lookups contain their
own retries, so discovery can exceed its nominal timeout.

The Run Control client's current 60-second boot deadline is shorter than
the minimum discovery timeout. The client can therefore stop waiting
while server-side work continues.

## Suggested follow-up

- Expose an overall boot timeout and align discovery with the remaining
  RPC time budget.
- Propagate cancellation and deadlines through application launches,
  discovery retries, network calls and waits.
- Explicitly cancel the client RPC when the user interrupts it.
- Stop further work on cancellation or deadline expiry; do not treat
  these as lookup failures that trigger fallback.
- Clear any previously stored controller address before discovery.
- Return explicit RPC errors when discovery fails, rather than returning
  success or no response.
- If boot success requires a reachable controller, verify it with a
  bounded controller RPC; a static address alone is not proof.
- Explain in timeout/cancellation messages that launched applications
  may still be running. No automatic rollback is proposed.

## Acceptance criteria

- Existing shells retain their discovery-time Ctrl+C behaviour.
- Run Control discovery does not register signals from worker threads.
- Missing-controller discovery finishes with a meaningful outcome.
- Cancellation and deadlines stop further work within a documented bound.
- Normal lookup timeout retains the intended static fallback.
- Failed discovery cannot reuse a stale controller address.
- Client-visible failures explain whether applications may remain running.