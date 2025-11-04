"""
Command-line interface for Manager gRPC Server

This module provides a standalone CLI for starting Manager servers.
It handles argument parsing and delegates to the main server runner function.
"""

import argparse
import sys

from tests.grpc_testing_tools.run_grpc_services import run_process_manager_server


def main():
    """
    Main entry point for Manager server CLI.

    Parses command-line arguments and starts the Manager server
    with the specified configuration parameters.
    """
    parser = argparse.ArgumentParser(
        description="Start a Manager gRPC server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Required arguments
    parser.add_argument(
        "--port",
        type=int,
        required=True,
        help="TCP port number for the Manager server to bind to",
    )
    parser.add_argument(
        "--workers",
        type=int,
        required=True,
        help="Maximum number of worker threads for the gRPC server",
    )
    parser.add_argument(
        "--log-file",
        required=True,
        help="Path to log file for server output and error messages",
    )

    args = parser.parse_args()

    try:
        print(f"Starting Manager server on port {args.port}")
        print(f"Using {args.workers} worker threads")
        print(f"Logging to: {args.log_file}")

        # Start the Manager server with parsed arguments
        run_process_manager_server(
            manager_max_workers=args.workers,
            server_port=args.port,
            log_file=args.log_file,
        )

    except KeyboardInterrupt:
        print("\nManager server interrupted by user")
    except Exception as e:
        print(f"Manager server failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
