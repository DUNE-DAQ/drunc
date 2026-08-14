#!/usr/bin/env python3
"""
Command-line interface for ChildController gRPC Server

This module provides a standalone CLI for starting ChildController servers.
It handles argument parsing and delegates to the main server runner function.
"""

import argparse
import sys

from drunc.grpc_testing_tools.run_grpc_services import run_child_controller_server


def main() -> None:
    """
    Main entry point for ChildController server CLI.

    Parses command-line arguments and starts the ChildController server
    with the specified configuration parameters.
    """
    parser = argparse.ArgumentParser(
        description="Start a ChildController gRPC server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Required arguments
    parser.add_argument(
        "--port",
        type=int,
        required=True,
        help="TCP port number for the ChildController server to bind to",
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
        "--root-port",
        type=int,
        required=True,
        help="Port number of the RootController server to connect to",
    )
    parser.add_argument(
        "--child-name",
        required=True,
        help="Unique identifier name for this ChildController instance",
    )

    args = parser.parse_args()

    try:
        print(f"Starting ChildController '{args.child_name}' on port {args.port}")
        print(f"Connecting to RootController on port {args.root_port}")
        print(f"Logging to: {args.log_file}")

        # Start the ChildController server with parsed arguments
        run_child_controller_server(
            controller_max_workers=args.workers,
            server_port=args.port,
            root_port=args.root_port,
            child_name=args.child_name,
            log_file=args.log_file,
        )

    except KeyboardInterrupt:
        print(f"\nChildController '{args.child_name}' interrupted by user")
    except Exception as e:
        print(f"ChildController '{args.child_name}' failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
