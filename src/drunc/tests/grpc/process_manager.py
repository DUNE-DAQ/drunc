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
        
        This method initiates a graceful shutdown of the Manager process
        by sending a SIGTERM signal to itself after a brief delay to allow
        the response to be sent back to the client.
        
        Args:
            request: KillRequest containing optional confirmation token and reason
            context: gRPC context object
            
        Returns:
            KillResponse indicating shutdown has been initiated
        """
        # Default grace period if not specified
        grace_period = max(request.grace_period_seconds, 1) if request.grace_period_seconds > 0 else 2
        
        # Log the shutdown request
        reason = request.reason or "No reason provided"
        print(f"Manager shutdown requested. Reason: {reason}")
        print(f"Grace period: {grace_period} seconds")
        
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
            shutdown_initiated=True,
            message="Manager shutdown initiated",
            estimated_shutdown_time_seconds=grace_period
        )