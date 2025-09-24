"""
Abstract gRPC Server Manager

Provides abstraction over gRPC server lifecycle management,
separating server-specific configuration and startup logic from the underlying
process execution mechanism (multiprocessing, SSH, ...).
"""

from abc import ABC, abstractmethod
from typing import List, Tuple, Any, Optional

from drunc.tests.grpc.process_connection_manager import ProcessConnectionManager, RunningGrpcServer


class GrpcServerConfig:
    """
    Configuration container for a gRPC server instance.
    
    Encapsulates all parameters needed to start and manage a gRPC server,
    providing a consistent interface across different execution environments.
    """
    
    def __init__(self,
                 server_id: str,
                 server_type: str,
                 port: int,
                 max_workers: int,
                 log_file: str,
                 server_options: List[Tuple[str, Any]] = None,
                 client_options: List[Tuple[str, Any]] = None,
                 **kwargs):
        """
        Initialise gRPC server configuration.
        
        Args:
            server_id: Unique identifier for this server instance
            server_type: Type of server ('manager', 'root_controller', 'child_controller')
            port: TCP port for the server to bind to
            max_workers: Maximum number of worker threads for the server
            log_file: Path to log file for server output
            server_options: gRPC server configuration options
            client_options: gRPC client configuration options (for servers that act as clients)
            **kwargs: Additional server-specific parameters
        """
        self.server_id = server_id
        self.server_type = server_type
        self.port = port
        self.max_workers = max_workers
        self.log_file = log_file
        self.server_options = server_options or []
        self.client_options = client_options or []
        
        # Store additional parameters for specific server types
        self.extra_params = kwargs
        
    def get_param(self, key: str, default: Any = None) -> Any:
        """Get an additional parameter value."""
        return self.extra_params.get(key, default)


class GrpcServerManager(ABC):
    """
    Abstract base class for managing gRPC server lifecycle.
    
    Defines the interface that all gRPC server management implementations must
    follow, whether using local multiprocessing or remote SSH execution.
    """
    
    def __init__(self, connection_manager: ProcessConnectionManager):
        """
        Initialise gRPC server manager.
        
        Args:
            connection_manager: Process execution manager (multiprocessing or SSH)
        """
        self.connection_manager = connection_manager
        
    @abstractmethod
    def start_manager_server(self, config: GrpcServerConfig) -> RunningGrpcServer:
        """
        Start a Manager gRPC server.
        
        Args:
            config: Configuration for the Manager server
            
        Returns:
            ProcessHandle for the started server
            
        Raises:
            RuntimeError: If server cannot be started
        """
        pass
        
    @abstractmethod
    def start_root_controller_server(self, config: GrpcServerConfig) -> RunningGrpcServer:
        """
        Start a RootController gRPC server.
        
        Args:
            config: Configuration for the RootController server
            
        Returns:
            ProcessHandle for the started server
            
        Raises:
            RuntimeError: If server cannot be started
        """
        pass
        
    @abstractmethod
    def start_child_controller_server(self, config: GrpcServerConfig) -> RunningGrpcServer:
        """
        Start a ChildController gRPC server.
        
        Args:
            config: Configuration for the ChildController server
            
        Returns:
            ProcessHandle for the started server
            
        Raises:
            RuntimeError: If server cannot be started
        """
        pass
        
    @abstractmethod
    def wait_for_server_ready(self, server_id: str, timeout: float = 10.0) -> bool:
        """
        Wait for a server to signal that it's ready to accept connections.
        
        Args:
            server_id: ID of the server to wait for
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if server became ready, False if timeout occurred
        """
        pass
        
    @abstractmethod
    def stop_server(self, server_id: str, timeout: float = 10.0) -> None:
        """
        Stop a running gRPC server gracefully.
        
        Args:
            server_id: ID of the server to stop
            timeout: Maximum time to wait for graceful shutdown
            
        Raises:
            RuntimeError: If server cannot be stopped
        """
        pass
        
    @abstractmethod
    def stop_all_servers(self, timeout: float = 10.0) -> None:
        """
        Stop all managed gRPC servers.
        
        Args:
            timeout: Maximum time to wait for each server to stop
        """
        pass
        
    @abstractmethod
    def get_server_handle(self, server_id: str) -> Optional[RunningGrpcServer]:
        """
        Get process handle for a managed server.
        
        Args:
            server_id: ID of the server
            
        Returns:
            ProcessHandle if server exists, None otherwise
        """
        pass
        
    @abstractmethod
    def is_server_running(self, server_id: str) -> bool:
        """
        Check if a server is currently running.
        
        Args:
            server_id: ID of the server to check
            
        Returns:
            True if server is running, False otherwise
        """
        pass
        
    @abstractmethod
    def cleanup(self) -> None:
        """Clean up all resources and stop remaining servers."""
        pass