import time
from typing import Any, Callable, Optional


def wait_for(
    condition: Callable[[], Any],
    expected_value: Any,
    timeout: float = 10.0,
    poll_interval: float = 0.1,
    logger: Optional[Any] = None,
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

    """
    start_time = time.time()
    last_value = None

    while time.time() - start_time < timeout:
        if logger is not None:
            logger.debug(f"Started waiting for condition to equal {expected_value}")
        try:
            last_value = condition()
        except Exception:
            time.sleep(poll_interval)
            continue

        # Handle callable expected_value (predicate function)
        if callable(expected_value):
            if expected_value(last_value):
                if logger is not None:
                    logger.debug(
                        f"Condition satisfied. Total wait time: {time.time() - start_time:.2f}s"
                    )
                return last_value
        # Handle tuple of acceptable values
        elif isinstance(expected_value, tuple):
            if last_value in expected_value:
                if logger is not None:
                    logger.debug(
                        f"Condition satisfied. Total wait time: {time.time() - start_time:.2f}s"
                    )
                return last_value
        # Handle None as "wait for any truthy value"
        elif expected_value is None:
            if last_value:
                if logger is not None:
                    logger.debug(
                        f"Condition satisfied. Total wait time: {time.time() - start_time:.2f}s"
                    )
                return last_value
        # Handle direct value comparison
        else:
            if last_value == expected_value:
                if logger is not None:
                    logger.debug(
                        f"Condition satisfied. Total wait time: {time.time() - start_time:.2f}s"
                    )
                return last_value

        time.sleep(poll_interval)
        if logger is not None:
            logger.debug(
                f"Waiting for condition: got {last_value}, "
                f"expecting {expected_value}. Elapsed time: {time.time() - start_time:.2f}s"
            )

    return None
