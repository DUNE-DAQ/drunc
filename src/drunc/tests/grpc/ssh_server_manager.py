"""
SSH-based server manager for coordinating server lifecycle.

This module provides simple server lifecycle coordination using
an SSH connection manager with pre-built boot commands.
"""

import socket
import time
from typing import Optional

from drunc.tests.grpc.grpc_server_manager import GrpcServerManager, GrpcServerConfig
from drunc.tests.grpc.process_connection_manager import RunningGrpcServer
from drunc.tests.grpc.ssh_connection_manager import SSHConnectionManager


class SSHGrpcServerManager(GrpcServerManager):
    """
    gRPC server manager using SSH for remote execution.
    
    Coordinates server lifecycle by delegating to SSH connection manager
    with pre-built boot commands. Does not handle command construction.
    """
    
    def __init__(self, connection_manager: SSHConnectionManager):
        """
        Initialise SSH gRPC server manager.
        
        Args:
            connection_manager: SSH connection manager with pre-built boot commands
        """
        super().__init__(connection_manager)
        
        # Track server handles for lifecycle management
        self.server_handles = {}
        
        print(f"SSH server manager initialised")
        
    def _check_port_accessibility(self, port: int, timeout: float = 1.0) -> bool:
        """
        Check if a port is accessible for gRPC connections.
        
        Args:
            port: Port number to check
            timeout: Connection timeout in seconds
            
        Returns:
            True if port is accessible, False otherwise
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(timeout)
                result = sock.connect_ex(('localhost', port))
                return result == 0
        except Exception:
            return False
        
    def start_manager_server(self, config: GrpcServerConfig) -> RunningGrpcServer:
        """
        Start Manager server using pre-built boot command.
        
        Args:
            config: Manager server configuration (only server_id is used)
            
        Returns:
            ProcessHandle for the remote Manager server process
            
        Raises:
            RuntimeError: If server cannot be started
        """
        try:
            # Create process handle
            handle = self.connection_manager.create_process(
                config.server_id,
                lambda: None,  # Placeholder function for SSH execution
            )
            
            # Set server_id for boot command lookup
            handle.set_server_id(config.server_id)
            
            # Track server handle before starting
            self.server_handles[config.server_id] = handle
            
            # Start the process using the standard start_process method
            self.connection_manager.start_process(handle)
            
            return handle
            
        except Exception as e:
            # Clean up tracking on failure
            if config.server_id in self.server_handles:
                del self.server_handles[config.server_id]
            raise RuntimeError(f"Failed to start Manager server {config.server_id}: {e}")
        
    def start_root_controller_server(self, config: GrpcServerConfig) -> RunningGrpcServer:
        """
        Start RootController server using pre-built boot command.
        
        Args:
            config: RootController server configuration (only server_id is used)
            
        Returns:
            ProcessHandle for the remote RootController server process
            
        Raises:
            RuntimeError: If server cannot be started
        """
        try:
            # Create process handle
            handle = self.connection_manager.create_process(
                config.server_id,
                lambda: None,  # Placeholder function for SSH execution
            )
            
            # Set server_id for boot command lookup
            handle.set_server_id(config.server_id)
            
            # Track server handle before starting
            self.server_handles[config.server_id] = handle
            
            # Start the process using the standard start_process method
            self.connection_manager.start_process(handle)
            
            return handle
            
        except Exception as e:
            # Clean up tracking on failure
            if config.server_id in self.server_handles:
                del self.server_handles[config.server_id]
            raise RuntimeError(f"Failed to start RootController server {config.server_id}: {e}")
        
    def start_child_controller_server(self, config: GrpcServerConfig) -> RunningGrpcServer:
        """
        Start ChildController server using pre-built boot command.
        
        Args:
            config: ChildController server configuration (only server_id is used)
            
        Returns:
            ProcessHandle for the remote ChildController server process
            
        Raises:
            RuntimeError: If server cannot be started
        """
        try:
            # Create process handle
            handle = self.connection_manager.create_process(
                config.server_id,
                lambda: None,  # Placeholder function for SSH execution
            )
            
            # Set server_id for boot command lookup
            handle.set_server_id(config.server_id)
            
            # Track server handle before starting
            self.server_handles[config.server_id] = handle
            
            # Start the process using the standard start_process method
            self.connection_manager.start_process(handle)
            
            return handle
            
        except Exception as e:
            # Clean up tracking on failure
            if config.server_id in self.server_handles:
                del self.server_handles[config.server_id]
            raise RuntimeError(f"Failed to start ChildController server {config.server_id}: {e}")
        
    def wait_for_server_ready(self, server_id: str, timeout: float = 10.0) -> bool:
        """
        Wait for a server to signal that it's ready to accept connections.
        
        Args:
            server_id: ID of the server to wait for
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if server is ready and port accessible, False otherwise
        """
        if server_id not in self.server_handles:
            print(f"Server {server_id} not found in server handles")
            return False
            
        handle = self.server_handles[server_id]
        
        # Get expected port from connection manager
        server_port = self.connection_manager.get_expected_port_for_server_id(server_id)
        
        if server_port is None:
            print(f"Error: Could not determine port for server {server_id}")
            return False
            
        start_time = time.time()
        last_status_time = start_time
        
        print(f"Waiting for server {server_id} to be accessible on port {server_port} (timeout: {timeout}s)...")
        
        # For SSH processes, give the remote command time to start before checking port
        initial_wait = min(2.0, timeout / 3)
        print(f"Initial wait of {initial_wait}s for remote process to start...")
        time.sleep(initial_wait)
        
        consecutive_successes = 0
        required_successes = 2  # Require 2 consecutive successful connections for stability
        
        while (time.time() - start_time) < timeout:
            # Check port accessibility (this is the primary indicator for SSH processes)
            if self._check_port_accessibility(server_port, timeout=1.0):
                consecutive_successes += 1
                if consecutive_successes >= required_successes:
                    print(f"✓ Server {server_id} is ready and accessible on port {server_port}")
                    return True
                else:
                    print(f"Port {server_port} accessible ({consecutive_successes}/{required_successes})")
            else:
                consecutive_successes = 0
                
                # Check if SSH process died very early (indicates startup failure)
                elapsed = time.time() - start_time
                if elapsed < initial_wait and not self.connection_manager.is_process_alive(handle):
                    startup_error = self.connection_manager.get_process_startup_error(handle)
                    if startup_error:
                        print(f"Server {server_id} failed during startup: {startup_error}")
                        return False
            
            # Periodic status updates
            elapsed = time.time() - start_time
            if elapsed - (last_status_time - start_time) >= 2.0:
                print(f"Waiting for server {server_id} port {server_port} (elapsed: {elapsed:.1f}s)")
                last_status_time = time.time()
            
            time.sleep(0.5)
            
        # Final timeout
        print(f"Timeout waiting for server {server_id} port {server_port} to be accessible")
        return False
        
    def stop_server(self, server_id: str, timeout: float = 10.0) -> None:
        """
        Stop a running gRPC server gracefully.
        
        Args:
            server_id: ID of the server to stop
            timeout: Maximum time to wait for graceful shutdown
        """
        if server_id not in self.server_handles:
            return
            
        print(f"Stopping server {server_id}...")
        handle = self.server_handles[server_id]
        
        # Use connection manager to stop the SSH process
        self.connection_manager.stop_process(handle, timeout=timeout)
        
        # Remove from tracking
        del self.server_handles[server_id]
        print(f"Server {server_id} stopped and removed from tracking")
            
    def stop_all_servers(self, timeout: float = 10.0) -> None:
        """
        Stop all managed gRPC servers.
        
        Args:
            timeout: Maximum time to wait for each server to stop
        """
        if not self.server_handles:
            print("No servers to stop")
            return
            
        print(f"Stopping {len(self.server_handles)} servers...")
        for server_id in list(self.server_handles.keys()):
            try:
                self.stop_server(server_id, timeout=timeout)
            except Exception as e:
                print(f"Warning: Error stopping server {server_id}: {e}")
                
    def get_server_handle(self, server_id: str) -> Optional[RunningGrpcServer]:
        """
        Get process handle for a managed server.
        
        Args:
            server_id: ID of the server
            
        Returns:
            ProcessHandle if server exists, None otherwise
        """
        return self.server_handles.get(server_id)
        
    def is_server_running(self, server_id: str) -> bool:
        """
        Check if a server is currently running.
        
        Args:
            server_id: ID of the server to check
            
        Returns:
            True if server is running, False otherwise
        """
        if server_id not in self.server_handles:
            return False
            
        handle = self.server_handles[server_id]
        is_alive = self.connection_manager.is_process_alive(handle)
        
        # Also check for startup errors
        if is_alive:
            startup_error = self.connection_manager.get_process_startup_error(handle)
            if startup_error:
                print(f"Server {server_id} has startup error despite being alive: {startup_error}")
                return False
                
        return is_alive
        
    def cleanup(self) -> None:
        """Clean up all resources and stop remaining servers."""
        print("SSH gRPC server manager cleanup starting...")
        self.stop_all_servers()
        self.connection_manager.cleanup()
        print("SSH gRPC server manager cleanup completed")