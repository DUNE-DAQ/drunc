#!/usr/bin/env python3
"""
Simple test process that writes periodic log messages.
Runs until terminated by SSH connection closure via SIGHUP.
"""

import os
import signal
import sys
import time

# Flag to track termination signal
terminated = False


def signal_handler(signum, frame):
    """Handle termination signals gracefully."""
    global terminated
    terminated = True


def main():
    """Main process loop that runs until SIGHUP received."""
    # Register SIGHUP handler for SSH connection termination
    signal.signal(signal.SIGHUP, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Get process identifier from command line or use default
    process_id = sys.argv[1] if len(sys.argv) > 1 else "unknown"

    print(f"[{process_id}] Process started", flush=True)
    print(f"[{process_id}] PID: {os.getpid()}", flush=True)

    # Write log messages every second until terminated
    iteration = 0
    while not terminated:
        iteration += 1
        print(f"[{process_id}] Heartbeat {iteration}", flush=True)
        time.sleep(1.0)

    print(
        f"[{process_id}] Process terminating after {iteration} iterations", flush=True
    )


if __name__ == "__main__":
    main()
