import time
from typing import Any, Callable, Optional


def wait_for(
    condition: Callable[[], Any],
    expected_value: Any,
    timeout: float = 10.0,
    poll_interval: float = 0.1,
) -> Optional[Any]:
    """
    Wait for a condition to return an expected value within a timeout period.

    Repeatedly evaluates the condition callable until it returns the expected value
    or the timeout is reached. Useful for polling operations in tests and async workflows.

    Args:
        condition: Callable that returns a value to check. Should take no arguments.
        expected_value: The value to wait for. If None, waits for any truthy value.
                       Can also be a tuple of acceptable values.
        timeout: Maximum time to wait in seconds before raising TimeoutError.
        poll_interval: Time in seconds between condition evaluations.

    Returns:
        The value returned by condition when it matches expected_value.

    Raises:
        TimeoutError: If the condition doesn't return expected_value within timeout.
    """
    start_time = time.time()
    last_value = None

    while time.time() - start_time < timeout:
        try:
            last_value = condition()
        except Exception:
            time.sleep(poll_interval)
            continue

        # Handle callable expected_value (predicate function)
        if callable(expected_value):
            if expected_value(last_value):
                return last_value
        # Handle tuple of acceptable values
        elif isinstance(expected_value, tuple):
            if last_value in expected_value:
                return last_value
        # Handle None as "wait for any truthy value"
        elif expected_value is None:
            if last_value:
                return last_value
        # Handle direct value comparison
        else:
            if last_value == expected_value:
                return last_value

        time.sleep(poll_interval)

    return None
