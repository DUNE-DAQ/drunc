import os
import signal
import subprocess
import time


def kill_process_on_port(port: int, timeout: float = 1.0) -> bool:
    """
    Find and kill any process listening on the specified port.

    Args:
        port: Port number to check and clean up
        timeout: Maximum time to wait for process termination

    Returns:
        True if a process was found and killed, False otherwise
    """
    try:
        # Find process ID listening on the port
        result = subprocess.run(
            ["lsof", "-ti", f":{port}"],
            capture_output=True,
            text=True,
            timeout=2.0,
        )

        if result.returncode != 0 or not result.stdout.strip():
            return False

        # Get PIDs (may be multiple)
        pids = [int(pid) for pid in result.stdout.strip().split("\n") if pid]

        for pid in pids:
            try:
                print(f"Killing process {pid} on port {port}")
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                # Process already gone
                continue
            except PermissionError:
                print(f"Warning: No permission to kill process {pid}")
                continue

        # Wait for processes to terminate
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Check if any processes still exist
            still_alive = False
            for pid in pids:
                try:
                    os.kill(pid, 0)  # Check if process exists
                    still_alive = True
                except ProcessLookupError:
                    pass

            if not still_alive:
                return True

            time.sleep(0.2)

        # Force kill if still alive
        for pid in pids:
            try:
                os.kill(pid, signal.SIGKILL)
                print(f"Force killed process {pid} on port {port}")
            except ProcessLookupError:
                pass

        return True

    except subprocess.TimeoutExpired:
        print(f"Warning: lsof timeout while checking port {port}")
        return False
    except FileNotFoundError:
        # lsof not available, try alternative method with netstat
        try:
            result = subprocess.run(
                ["netstat", "-tlnp"],
                capture_output=True,
                text=True,
                timeout=2.0,
            )

            for line in result.stdout.split("\n"):
                if f":{port}" in line and "LISTEN" in line:
                    # Extract PID from netstat output
                    parts = line.split()
                    if len(parts) >= 7:
                        pid_program = parts[6]
                        if "/" in pid_program:
                            pid = int(pid_program.split("/")[0])
                            try:
                                print(
                                    f"Killing process {pid} on port {port} (via netstat)"
                                )
                                os.kill(pid, signal.SIGTERM)
                                try:
                                    os.kill(pid, signal.SIGKILL)
                                except ProcessLookupError:
                                    pass
                                return True
                            except (ProcessLookupError, PermissionError):
                                pass

        except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
            pass

        return False
    except Exception as e:
        print(f"Warning: Error killing process on port {port}: {e}")
        return False
