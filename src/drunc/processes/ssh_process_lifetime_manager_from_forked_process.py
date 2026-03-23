"""
Provides a subprocess-isolated wrapper around SSHProcessLifetimeManagerShell.

All method calls are forwarded to an SSHProcessLifetimeManagerShell instance
running in a dedicated child process via multiprocessing queues, keeping SSH
activity and threading state fully isolated from the parent process.
"""

import logging
import logging.handlers
import multiprocessing
import threading
import types
import uuid as _uuid_module
from typing import Any, Callable, Dict, List, Optional

from druncschema.process_manager_pb2 import BootRequest

from drunc.processes.ssh_process_lifetime_manager import ProcessLifetimeManager
from drunc.processes.ssh_process_lifetime_manager_shell import (
    SSHProcessLifetimeManagerShell,
)
from drunc.utils.utils import get_logger

# ---------------------------------------------------------------------------
# Worker process entry point (module-level so it is picklable by multiprocessing)
# ---------------------------------------------------------------------------


class _SSHProcessLifetimeManagerShellWithBytesEntry(SSHProcessLifetimeManagerShell):
    """
    Internal subclass used exclusively inside the forked worker process.

    Adds a bytes-based entry point for start_process() so that BootRequest
    protobuf objects can be transmitted across the process boundary as raw
    bytes rather than relying on protobuf pickling support.
    """

    def _start_process_from_bytes(self, uuid: str, boot_request_bytes: bytes) -> None:
        """
        Deserialise a BootRequest from bytes and start the process.

        Args:
            uuid:               Unique identifier for this process.
            boot_request_bytes: BootRequest serialised via SerializeToString().
        """
        boot_request = BootRequest()
        boot_request.ParseFromString(boot_request_bytes)
        self.start_process(uuid, boot_request)


def _resolve_parent_log_handlers() -> List[logging.Handler]:
    """
    Resolve handlers used by the parent QueueListener for child log records.

    Preference order:
      1) drunc.process_manager handlers
      2) drunc handlers
      3) root handlers
      4) StreamHandler fallback
    """
    for logger_name in ("drunc.process_manager", "drunc"):
        candidate_logger = logging.getLogger(logger_name)
        if candidate_logger.handlers:
            return list(candidate_logger.handlers)

    root_handlers = list(logging.getLogger().handlers)
    return root_handlers if root_handlers else [logging.StreamHandler()]


def _configure_child_logging_to_queue(log_queue: multiprocessing.Queue) -> None:
    """
    Configure child-process logging to forward all records via QueueHandler.

    Clears inherited handlers from root and drunc* loggers to avoid duplicate
    output and enforce a single logging path:
        child logger -> root -> QueueHandler -> parent QueueListener
    """
    queue_handler = logging.handlers.QueueHandler(log_queue)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers.clear()
    root_logger.addHandler(queue_handler)

    # Clear inherited handlers from drunc hierarchy in the forked child and
    # keep propagation enabled so records bubble up to root QueueHandler.
    for logger_name in list(logging.root.manager.loggerDict.keys()):
        if logger_name == "drunc" or logger_name.startswith("drunc."):
            child_logger = logging.getLogger(logger_name)
            child_logger.handlers.clear()
            child_logger.propagate = True


def _worker_process_main(
    request_queue: multiprocessing.Queue,
    response_queue: multiprocessing.Queue,
    callback_queue: multiprocessing.Queue,
    log_queue: multiprocessing.Queue,
    disable_host_key_check: bool,
    disable_localhost_host_key_check: bool,
) -> None:
    """
    Entry point executed inside the forked child process.

    Creates an SSHProcessLifetimeManagerShell instance and runs an event loop
    that receives (request_id, method_name, args, kwargs) tuples from
    request_queue, dispatches them to the manager, and places
    (request_id, result, error) tuples onto response_queue.

    Process-exit callbacks are forwarded to the parent via callback_queue as
    (uuid, exit_code, exception_string) tuples.

    A None sentinel placed on request_queue causes a clean shutdown.

    Args:
        request_queue:                    Inbound method-call requests from parent.
        response_queue:                   Outbound method-call results to parent.
        callback_queue:                   Outbound process-exit events to parent.
        disable_host_key_check:           Forwarded to SSHProcessLifetimeManagerShell.
        disable_localhost_host_key_check: Forwarded to SSHProcessLifetimeManagerShell.
    """
    # ------------------------------------------------------------------
    # Redirect all log records from this child process to
    # the parent via the shared queue. The QueueListener in the parent
    # will forward them to the real handlers, so all child logs appear
    # as if they originated from the parent process.
    # ------------------------------------------------------------------
    _configure_child_logging_to_queue(log_queue)

    def _on_process_exit(
        uuid: str,
        exit_code: Optional[int],
        exception: Optional[Exception],
    ) -> None:
        """Relay process-exit events back to the parent via the callback queue."""
        try:
            callback_queue.put_nowait(
                (uuid, exit_code, str(exception) if exception is not None else None)
            )
        except Exception:
            pass  # Never raise inside a background callback

    manager = _SSHProcessLifetimeManagerShellWithBytesEntry(
        disable_host_key_check=disable_host_key_check,
        disable_localhost_host_key_check=disable_localhost_host_key_check,
        on_process_exit=_on_process_exit,
    )

    # Attach the bytes-based start_process entry point so BootRequest objects
    # can be transmitted as raw bytes rather than relying on protobuf pickling.
    def _start_process_from_bytes(
        self_inner: SSHProcessLifetimeManagerShell, uuid: str, boot_request_bytes: bytes
    ) -> None:
        boot_request = BootRequest()
        boot_request.ParseFromString(boot_request_bytes)
        self_inner.start_process(uuid, boot_request)

    manager._start_process_from_bytes = types.MethodType(
        _start_process_from_bytes, manager
    )

    # IPC event loop: receive requests, dispatch, send responses.
    while True:
        try:
            request = request_queue.get()
        except (EOFError, OSError):
            # Parent closed the queue – exit cleanly.
            break

        # None is the shutdown sentinel.
        if request is None:
            break

        request_id, method_name, args, kwargs = request

        try:
            method = getattr(manager, method_name)
            result = method(*args, **kwargs)
            response_queue.put((request_id, result, None))
        except Exception as exc:
            # Ship a serialisable representation of the exception to the parent.
            response_queue.put((request_id, None, (type(exc).__name__, str(exc))))


class SSHProcessLifetimeManagerShellOnForkedProcess(ProcessLifetimeManager):
    """
    A ProcessLifetimeManager that delegates process management to an
    SSHProcessLifetimeManagerShell instance running in a dedicated child process.

    All public methods forward their call to the child process via multiprocessing
    queues and block until a response is received. This isolates SSH connections,
    threads, and file-descriptor state from the parent process, which is useful
    when the parent uses fork-unsafe libraries or needs clean process boundaries.

    _call() is safe to invoke from multiple threads simultaneously.
    Pending requests are matched to their responses by unique request IDs, and a
    dedicated response-dispatcher thread routes each response to the correct
    waiting caller without any cross-caller interference.
    """

    def __init__(
        self,
        disable_host_key_check: bool = False,
        disable_localhost_host_key_check: bool = False,
        logger: Optional[logging.Logger] = None,
        on_process_exit: Optional[
            Callable[[str, Optional[int], Optional[Exception]], None]
        ] = None,
    ) -> None:
        """
        Initialise the forked-process manager and start the child process immediately.

        Args:
            disable_host_key_check:
                Disable SSH host key verification for all hosts.
            disable_localhost_host_key_check:
                Disable SSH host key verification for localhost connections only.
            logger:
                Logger instance used by the parent process. The child creates
                its own independent logger.
            on_process_exit:
                Optional callback invoked in the *parent* process when a managed
                process exits. Signature: (uuid, exit_code, exception).
                The exception is reconstructed as a RuntimeError from the
                serialised message forwarded by the child process.
        """
        self.log = logger if logger is not None else get_logger(__name__)
        self._on_process_exit = on_process_exit

        # Queues for IPC between parent and child.
        self._request_queue: multiprocessing.Queue = multiprocessing.Queue()
        self._response_queue: multiprocessing.Queue = multiprocessing.Queue()
        self._callback_queue: multiprocessing.Queue = multiprocessing.Queue()

        # Dedicated queue for log records forwarded from the child process.
        # A QueueListener in the parent drains this queue and dispatches
        # records to drunc hierarchy handlers, with root fallback.
        self._log_queue: multiprocessing.Queue = multiprocessing.Queue()
        self._log_listener = logging.handlers.QueueListener(
            self._log_queue,
            *_resolve_parent_log_handlers(),
            respect_handler_level=True,
        )

        # Maps request_id -> dict with event + result storage, used to match
        # asynchronous queue responses back to their blocking callers.
        self._pending: Dict[str, Dict[str, Any]] = {}
        self._pending_lock = threading.Lock()

        # Start the child process.
        self._worker = multiprocessing.Process(
            target=_worker_process_main,
            args=(
                self._request_queue,
                self._response_queue,
                self._callback_queue,
                self._log_queue,
                disable_host_key_check,
                disable_localhost_host_key_check,
            ),
            daemon=True,
            name="SSHLifetimeManagerWorker",
        )
        self._worker.start()
        self.log.debug(
            f"SSHProcessLifetimeManagerShell worker process started (PID {self._worker.pid})"
        )

        # Start listener thread only after forking worker to avoid forking
        # while this parent thread is already running.
        self._log_listener.start()

        # Background thread that routes response messages to their waiting callers.
        self._response_dispatcher = threading.Thread(
            target=self._run_response_dispatcher,
            name="SSHLifetimeManagerResponseDispatcher",
            daemon=True,
        )
        self._response_dispatcher.start()

        # Background thread that invokes on_process_exit for events forwarded
        # by the child via the callback queue.
        self._callback_dispatcher = threading.Thread(
            target=self._run_callback_dispatcher,
            name="SSHLifetimeManagerCallbackDispatcher",
            daemon=True,
        )
        self._callback_dispatcher.start()

    def _call(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        """
        Send a method call to the child process and block until the result arrives.

        A unique request ID is generated per call and used by the response-dispatcher
        thread to route the response back to the correct waiting caller. Multiple
        threads may invoke this method concurrently without interference.

        Args:
            method_name: Name of the method to invoke on the child's manager instance.
            *args:        Positional arguments forwarded verbatim to the method.
            **kwargs:     Keyword arguments forwarded verbatim to the method.

        Returns:
            The return value produced by the method in the child process.

        Raises:
            RuntimeError: If the child process raised an exception, or if the
                          worker process is no longer running.
        """
        if not self._worker.is_alive():
            raise RuntimeError(
                "SSHProcessLifetimeManager worker process is no longer running."
            )

        request_id = str(_uuid_module.uuid4())
        event = threading.Event()

        # Register the pending slot *before* enqueuing the request to eliminate
        # the race where a fast response arrives before registration completes.
        with self._pending_lock:
            self._pending[request_id] = {
                "event": event,
                "result": None,
                "error": None,
            }

        self._request_queue.put((request_id, method_name, args, kwargs))

        # Block the calling thread until the dispatcher sets the event.
        event.wait()

        with self._pending_lock:
            slot = self._pending.pop(request_id)

        if slot["error"] is not None:
            exc_type_name, exc_message = slot["error"]
            raise RuntimeError(
                f"Child process raised {exc_type_name} in {method_name}(): {exc_message}"
            )

        return slot["result"]

    def _run_response_dispatcher(self) -> None:
        """
        Continuously read responses from the response queue and wake the correct
        waiting caller by setting its threading.Event.

        Runs in a dedicated daemon thread for the lifetime of this object.
        Exits cleanly on a None sentinel or if the queue is closed.
        """
        while True:
            try:
                message = self._response_queue.get()
            except (EOFError, OSError):
                break

            # None sentinel signals this thread to exit (sent by shutdown()).
            if message is None:
                break

            response_id, result, error = message

            with self._pending_lock:
                slot = self._pending.get(response_id)

            if slot is not None:
                slot["result"] = result
                slot["error"] = error
                slot["event"].set()
            else:
                self.log.warning(
                    f"Response dispatcher received unknown request_id '{response_id}'; "
                    f"response will be discarded."
                )

    def _run_callback_dispatcher(self) -> None:
        """
        Continuously read process-exit events forwarded by the child process and
        invoke the user-supplied on_process_exit callback in the parent process.

        Runs in a dedicated daemon thread for the lifetime of this object.
        Exits cleanly on a None sentinel or if the queue is closed.
        """
        while True:
            try:
                message = self._callback_queue.get()
            except (EOFError, OSError):
                break

            # None sentinel signals this thread to exit (sent by shutdown()).
            if message is None:
                break

            uuid, exit_code, exception_string = message

            if self._on_process_exit is not None:
                try:
                    # Reconstruct the exception from its serialised string form.
                    exception: Optional[Exception] = (
                        RuntimeError(exception_string)
                        if exception_string is not None
                        else None
                    )
                    self._on_process_exit(uuid, exit_code, exception)
                except Exception as exc:
                    self.log.error(
                        f"Error in on_process_exit callback for process {uuid}: {exc}"
                    )

    def shutdown(self) -> None:
        """
        Gracefully shut down the child process and all background dispatcher threads.

        Sends shutdown sentinels to each queue so the child's event loop and both
        dispatcher threads exit cleanly. Joins the child process with a short
        timeout and forcibly terminates it if it does not exit in time.
        """
        self.log.debug("Shutting down SSHProcessLifetimeManager worker process...")

        # Signal the child's event loop to exit.
        try:
            self._request_queue.put(None)
        except Exception:
            pass

        # Signal the dispatcher threads to exit.
        try:
            self._response_queue.put(None)
        except Exception:
            pass

        try:
            self._callback_queue.put(None)
        except Exception:
            pass

        self._worker.join(timeout=5.0)

        if self._worker.is_alive():
            self.log.warning(
                "Worker process did not exit within the timeout; terminating forcibly."
            )
            self._worker.terminate()
            self._worker.join(timeout=2.0)

        # Stop listener last so final child log records are drained.
        try:
            self._log_listener.stop()
        except Exception:
            pass

        self.log.debug("SSHProcessLifetimeManager worker process shut down.")

    # ------------------------------------------------------------------
    # ProcessLifetimeManager interface – all methods delegate to child
    # ------------------------------------------------------------------

    def get_active_process_keys(self) -> List[str]:
        """
        Get list of active process UUIDs from the child process.

        Returns:
            List of active process UUID strings.
        """
        return self._call("get_active_process_keys")

    def start_process(self, uuid: str, boot_request: BootRequest) -> None:
        """
        Start a remote process in the child process using the boot request configuration.

        The BootRequest protobuf message is serialised to bytes before being sent
        across the process boundary and deserialised inside the child, avoiding any
        reliance on protobuf pickling support.

        Args:
            uuid:         Unique identifier for this process.
            boot_request: BootRequest containing process configuration.

        Raises:
            RuntimeError: If the child process raises during process startup.
        """
        boot_request_bytes = boot_request.SerializeToString()
        self._call("_start_process_from_bytes", uuid, boot_request_bytes)

    def is_process_alive(self, uuid: str) -> bool:
        """
        Check if a managed process is alive.

        Args:
            uuid: Process UUID to check.

        Returns:
            True if the process is alive, False otherwise.
        """
        return self._call("is_process_alive", uuid)

    def pop_early_exit_code(self, uuid: str) -> Optional[int]:
        """
        Retrieve and remove the exit code of a process that exited unexpectedly.

        Args:
            uuid: Process UUID.

        Returns:
            Exit code if the process terminated early without being explicitly killed,
            None if still running or not found.
        """
        return self._call("pop_early_exit_code", uuid)

    def kill_process(
        self,
        uuid: str,
        timeout: float = ProcessLifetimeManager.DEFAULT_TIMEOUT_FOR_KILLING_PROCESS,
    ) -> Optional[int]:
        """
        Kill a remote process and clean up its resources.

        Args:
            uuid:    Process UUID to terminate.
            timeout: Graceful termination timeout in seconds.

        Returns:
            Exit code of the terminated process, or None if undetermined.
        """
        return self._call("kill_process", uuid, timeout)

    def crash_process(self, uuid: str) -> None:
        """
        Simulate a process crash by sending SIGKILL without performing any cleanup.

        Delegates to the underlying SSHProcessLifetimeManagerShell running in
        the forked worker process. Sends SIGKILL to the remote process without
        cleaning up any associated resources, simulating an unexpected crash.

        Args:
            uuid: Process UUID to crash
        """
        self._call("crash_process", uuid)

    def kill_processes(
        self,
        uuids: List[str],
        process_timeouts: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Optional[int]]:
        """
        Kill multiple processes in role-based shutdown order.

        Args:
            uuids:            List of process UUIDs to terminate.
            process_timeouts: Optional per-UUID timeout overrides in seconds.

        Returns:
            Dictionary mapping process UUIDs to their exit codes.
        """
        return self._call("kill_processes", uuids, process_timeouts)

    def kill_all_processes(
        self,
        process_timeouts: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Optional[int]]:
        """
        Kill all managed processes.

        Args:
            process_timeouts: Optional per-UUID timeout overrides in seconds.

        Returns:
            Dictionary mapping all process UUIDs to their exit codes.
        """
        return self._call("kill_all_processes", process_timeouts)

    def kill_processes_by_role(
        self,
        role: str,
        candidate_uuids: List[str],
        process_timeouts: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Optional[int]]:
        """
        Kill all processes with the specified role from the candidate UUID list.

        Args:
            role:             Process role to match (e.g. "controller", "application").
            candidate_uuids:  List of process UUIDs to filter by role.
            process_timeouts: Optional per-UUID timeout overrides in seconds.

        Returns:
            Dictionary mapping terminated process UUIDs to their exit codes.
        """
        return self._call(
            "kill_processes_by_role", role, candidate_uuids, process_timeouts
        )

    def get_process_stdout(self, uuid: str) -> Optional[str]:
        """
        Get accumulated stdout from a managed process.

        Args:
            uuid: Process UUID.

        Returns:
            Stdout content as a string, or None if not available.
        """
        return self._call("get_process_stdout", uuid)

    def get_process_stderr(self, uuid: str) -> Optional[str]:
        """
        Get accumulated stderr from a managed process.

        Args:
            uuid: Process UUID.

        Returns:
            Stderr content as a string, or None if not available.
        """
        return self._call("get_process_stderr", uuid)

    def read_log_file(
        self,
        hostname: str,
        user: str,
        log_file: str,
        num_lines: int = 100,
    ) -> List[str]:
        """
        Read the last N lines of a remote log file via the child process.

        Args:
            hostname:  Target hostname.
            user:      SSH username.
            log_file:  Remote log file path.
            num_lines: Number of lines to retrieve from the end of the file.

        Returns:
            List of log lines.
        """
        return self._call("read_log_file", hostname, user, log_file, num_lines)

    def validate_host_connection(
        self,
        host: str,
        auth_method: str,
        user: str,
    ) -> None:
        """
        Validate an SSH connection to the specified host via the child process.

        Args:
            host:        Target hostname.
            auth_method: Authentication method (passed through to inner manager).
            user:        SSH username.

        Raises:
            RuntimeError: If the SSH connection validation fails.
        """
        self._call("validate_host_connection", host, auth_method, user)
