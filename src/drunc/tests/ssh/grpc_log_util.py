import os
import sys
import threading


def stderr_observer(log_file_name):
    """
    Redirect stderr to a log file for process output capture.

    Args:
        log_file_name: Path to log file for stderr redirection
    """
    r, w = os.pipe()
    stderr_fd = sys.stderr.fileno()
    os.dup2(w, stderr_fd)
    os.close(w)

    def reader():
        log_handle = open(log_file_name, "w", buffering=1)
        with os.fdopen(r) as pipe:
            for line in pipe:
                log_handle.write(f"{line.strip()}\n")
                log_handle.flush()
        log_handle.close()

    reader_thread = threading.Thread(target=reader, daemon=True)
    reader_thread.start()


def stdout_observer(log_file_name):
    """
    Redirect stdout to a log file for process output capture.

    Args:
        log_file_name: Path to log file for stdout redirection
    """
    r, w = os.pipe()
    stdout_fd = sys.stdout.fileno()
    os.dup2(w, stdout_fd)
    os.close(w)

    def reader():
        log_handle = open(log_file_name, "a", buffering=1)
        with os.fdopen(r) as pipe:
            for line in pipe:
                log_handle.write(f"{line.strip()}\n")
                log_handle.flush()
        log_handle.close()

    reader_thread = threading.Thread(target=reader, daemon=True)
    reader_thread.start()
