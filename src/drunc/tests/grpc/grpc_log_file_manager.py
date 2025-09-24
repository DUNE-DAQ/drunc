import os
import re
from typing import List
import time
import tempfile
from pathlib import Path

# gRPC error patterns to detect in log files
GRPC_ERROR_PATTERNS = [
    "ping_timeout",
    "keepalive.*timeout",
    "chttp2_transport.*GOAWAY",
    "GOAWAY.*UNAVAILABLE",
    "Error code.*ping_timeout",
    "grpc.*UNAVAILABLE.*ping",
    "Other threads are currently calling into gRPC"
]

class LogFileManager:
    """
    Manager for process log files with automatic creation and cleanup.

    Creates unique log files in /tmp for each process and ensures proper
    cleanup after test completion to prevent file system clutter.
    """

    def __init__(self):
        """Initialise log file manager with empty state."""
        self.log_files = []
        self.file_positions = {}
        self.temp_dir = None

    def create_log_file(self, process_name: str) -> str:
        """
        Create a unique log file for a process.

        Args:
            process_name: Name of the process requiring logging

        Returns:
            Full path to the created log file
        """
        if not self.temp_dir:
            # Create temporary directory for all test log files
            self.temp_dir = tempfile.mkdtemp(prefix="grpc_test_logs_")

        # Generate unique log file path with timestamp
        timestamp = int(time.time())
        log_file = os.path.join(self.temp_dir, f"{process_name}_{timestamp}.log")

        # Create empty log file
        Path(log_file).touch()
        self.log_files.append(log_file)
        self.file_positions[log_file] = 0

        return log_file

    def get_all_log_files(self) -> List[str]:
        """
        Retrieve list of all created log files.

        Returns:
            List of absolute paths to all created log files
        """
        return self.log_files.copy()

    def cleanup(self):
        """Remove all log files and temporary directory."""
        for log_file in self.log_files:
            try:
                if os.path.exists(log_file):
                    os.remove(log_file)
            except Exception as e:
                print(f"Warning: Could not remove log file {log_file}: {e}")

        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                os.rmdir(self.temp_dir)
            except Exception as e:
                print(f"Warning: Could not remove temp directory {self.temp_dir}: {e}")

        self.log_files.clear()
        self.file_positions.clear()
        self.temp_dir = None

    def _scan_content_for_errors(self, content: str) -> List[str]:
        """
        Scan text content for gRPC error patterns.

        Args:
            content: Text content to scan for error patterns

        Returns:
            List of lines containing detected error patterns
        """
        detected_errors = []

        for line in content.split("\n"):
            line = line.strip()
            if not line:
                continue

            # Check each error pattern against the line
            for pattern in GRPC_ERROR_PATTERNS:
                if re.search(pattern, line, re.IGNORECASE):
                    detected_errors.append(line)
                    break  # Avoid duplicate detection of same line

        return detected_errors

    def check_for_errors(self):
        """
        Check all log files for gRPC error patterns.
        
        Returns:
            Error details if found, None otherwise
        """
        for log_file in self.get_all_log_files():
            if not os.path.exists(log_file):
                continue

            # Read new content since last check
            try:
                with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                    f.seek(self.file_positions[log_file])
                    new_content = f.read()
                    self.file_positions[log_file] = f.tell()
            except (IOError, OSError):
                # File may not be ready yet, continue monitoring
                continue

            if new_content:
                # Check new content for error patterns
                error_lines = self._scan_content_for_errors(new_content)
                if error_lines:
                    # Store error details and signal detection
                    self.detected_error = {
                        "file": log_file,
                        "lines": error_lines,
                    }
                    return self.detected_error
        return None

