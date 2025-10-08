#!/usr/bin/env python3
"""
Command-line interface for RootController gRPC Server

This module provides a standalone CLI for starting RootController servers.
It handles argument parsing and delegates to the main server runner function.
"""

import argparse
import sys

from drunc.tests.grpc_testing_tools.run_grpc_services import run_root_controller_server


def main():
    """
    Main entry point for RootController server CLI.

    Parses command-line arguments and starts the RootController server
    with the specified configuration parameters.
    """
    parser = argparse.ArgumentParser(
        description="Start a RootController gRPC server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Required arguments
    parser.add_argument(
        "--port",
        type=int,
        required=True,
        help="TCP port number for the RootController server to bind to",
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
    parser.add_argument(
        "--manager-port",
        type=int,
        required=True,
        help="Port number of the Manager server to connect to",
    )

    args = parser.parse_args()

    try:
        print(f"Starting RootController on port {args.port}")
        print(f"Connecting to Manager on port {args.manager_port}")
        print(f"Logging to: {args.log_file}")

        # Start the RootController server with parsed arguments
        run_root_controller_server(
            controller_max_workers=args.workers,
            server_port=args.port,
            manager_port=args.manager_port,
            log_file=args.log_file,
        )

    except KeyboardInterrupt:
        print("\nRootController interrupted by user")
    except Exception as e:
        print(f"RootController failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
