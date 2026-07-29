"""
ChildController Service Implementation

Provides the gRPC servicer implementation for ChildController services,
which act as leaf nodes in the system hierarchy, handling specific tasks
and reporting to the RootController.
"""

import os
import signal
import threading
from typing import Protocol, cast

import grpc

from drunc.grpc_testing_tools import test_services_pb2 as pb2
from drunc.grpc_testing_tools.test_services_pb2_grpc import (
    ChildControllerServiceServicer,
)


class _Pb2ModuleProtocol(Protocol):
    def DummyResponse(self, *args: object, **kwargs: object) -> object: ...

    def KillResponse(self, *args: object, **kwargs: object) -> object: ...


PB2 = cast(_Pb2ModuleProtocol, pb2)


class ChildControllerServiceImpl(ChildControllerServiceServicer):
    """
    Implementation of ChildController gRPC service.

    ChildController services are leaf nodes that handle specific tasks
    while maintaining connections to their RootController. Each child
    has a unique name identifier and handles connectivity testing,
    instruction processing, and graceful shutdown requests.
    """

    def __init__(self, name: str) -> None:
        """
        Initialise the ChildController service implementation.

        Args:
            name: Unique identifier for this child controller instance
        """
        self.name = name

    def MakeRequest(self, request: object, context: grpc.ServicerContext) -> object:
        """
        Handle incoming connectivity test requests.

        Args:
            request: DummyRequest containing message and timestamp
            context: gRPC context object

        Returns:
            DummyResponse with echoed message confirming ChildController is responsive
        """
        message = getattr(request, "message", "")
        return PB2.DummyResponse(reply=f"{self.name} server response: {message}")

    def Kill(self, request: object, context: grpc.ServicerContext) -> object:
        grace_period = (
            max(getattr(request, "grace_period_seconds", 0), 1)
            if getattr(request, "grace_period_seconds", 0) > 0
            else 2
        )

        # Build detailed response message
        reason = getattr(request, "reason", None) or "No reason provided"
        response_details = [
            "Manager Kill method executed successfully",
            f"Reason: {reason}",
            f"Grace period: {grace_period}s",
            f"PID: {os.getpid()}",
            "Shutdown thread starting...",
        ]

        def delayed_shutdown() -> None:
            """Send SIGTERM to this process after a brief delay."""
            import time

            time.sleep(0.5)  # Allow response to be sent
            os.kill(os.getpid(), signal.SIGTERM)

        # Start shutdown in separate thread to avoid blocking the response
        shutdown_thread = threading.Thread(target=delayed_shutdown)
        shutdown_thread.daemon = True
        shutdown_thread.start()

        return PB2.KillResponse(
            shutdown_initiated=True, message=" | ".join(response_details)
        )
