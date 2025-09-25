"""
Manager Service Implementation

Provides the gRPC servicer implementation for the Manager service,
which acts as the top-level coordinator in the system hierarchy.
"""

import os
import signal
import threading

from drunc.tests.grpc.test_pb2 import (
    DummyResponse,
    KillRequest,
    KillResponse,
)
from drunc.tests.grpc.test_pb2_grpc import ManagerServiceServicer


class ManagerServiceImpl(ManagerServiceServicer):
    """
    Implementation of Manager gRPC service.

    The Manager service acts as the top-level coordinator and does not
    connect to any upstream services. It handles basic connectivity
    testing and graceful shutdown requests.
    """

    def __init__(self):
        """Initialise the Manager service implementation."""
        pass

    def MakeRequest(self, request, context):
        """
        Handle incoming connectivity test requests.

        Args:
            request: DummyRequest containing message and timestamp
            context: gRPC context object

        Returns:
            DummyResponse with echoed message confirming Manager is responsive
        """
        return DummyResponse(reply=f"Manager server response: {request.message}")

    def Kill(self, request: KillRequest, context) -> KillResponse:
        """
        Handle graceful shutdown requests for the Manager service.
        """

        grace_period = (
            max(request.grace_period_seconds, 1)
            if request.grace_period_seconds > 0
            else 2
        )

        # Build detailed response message
        reason = request.reason or "No reason provided"
        response_details = [
            "Manager Kill method executed successfully",
            f"Reason: {reason}",
            f"Grace period: {grace_period}s",
            f"PID: {os.getpid()}",
            "Shutdown thread starting...",
        ]

        def delayed_shutdown():
            """Send SIGTERM to this process after a brief delay."""
            import time

            time.sleep(0.5)  # Allow response to be sent
            os.kill(os.getpid(), signal.SIGTERM)

        # Start shutdown in separate thread to avoid blocking the response
        shutdown_thread = threading.Thread(target=delayed_shutdown)
        shutdown_thread.daemon = True
        shutdown_thread.start()

        return KillResponse(
            shutdown_initiated=True, message=" | ".join(response_details)
        )
