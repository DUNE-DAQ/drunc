#!/usr/bin/env python3
"""
Remote CLI Command Builder

This module provides a high-level interface for constructing complete SSH commands
to start gRPC servers remotely. It handles all command construction including
SSH arguments, environment setup, and Python execution.
"""

import os
from typing import Dict, List
from dataclasses import dataclass


@dataclass
class BootServerCommand:
    """
    Complete SSH command specification for starting a gRPC server remotely.
    
    Contains the fully constructed SSH command ready for execution,
    including all SSH options, environment setup, and server parameters.
    """
    server_id: str
    complete_ssh_command: List[str]  # Complete SSH command as argument list
    description: str
    expected_port: int
    
    def __str__(self) -> str:
        """Return a string representation of the boot command."""
        return f"BootServerCommand({self.server_id}, port={self.expected_port})"


class RemoteCLICommandBuilder:
    """
    Builder for constructing complete SSH commands to start gRPC servers remotely.
    
    Handles all aspects of command construction including SSH connection parameters,
    environment script sourcing, and Python server execution. Produces complete
    SSH commands ready for execution.
    """
    
    def __init__(self, 
                 env_setup_script: str,
                 python_executable: str = "python3",
                 working_directory: str = None,
                 cli_script_path: str = None,
                 default_user: str = None,
                 hosts: List[str] = None,
                 disable_host_key_check: bool = False,
                 ssh_options: List[str] = None,
                 env_vars: Dict[str, str] = None):
        """
        Initialise the remote CLI command builder.
        
        Args:
            env_setup_script: Path to shell script that sets up the Python environment
            python_executable: Python interpreter to use on remote hosts
            working_directory: Working directory for remote execution
            cli_script_path: Path to directory containing CLI scripts
            default_user: Default username for SSH connections
            hosts: List of available hosts for process execution
            disable_host_key_check: Disable SSH host key verification
            ssh_options: Additional SSH command-line options
            env_vars: Additional environment variables to export on remote hosts
        """
        self.env_setup_script = env_setup_script
        self.python_executable = python_executable
        self.working_directory = working_directory or os.getcwd()
        self.cli_script_path = cli_script_path or self._find_cli_script_path()
        self.default_user = default_user or os.getenv('USER', 'root')
        self.hosts = hosts or ['localhost']
        self.disable_host_key_check = disable_host_key_check
        self.ssh_options = ssh_options or []
        self.env_vars = env_vars or {}
        
        print(f"Remote CLI command builder initialised:")
        print(f"  Environment script: {self.env_setup_script}")
        print(f"  Python executable: {self.python_executable}")
        print(f"  Working directory: {self.working_directory}")
        print(f"  Available hosts: {self.hosts}")
        
    def _find_cli_script_path(self) -> str:
        """
        Find the CLI scripts directory relative to this module.
        
        Returns:
            Path to directory containing CLI scripts
            
        Raises:
            FileNotFoundError: If required CLI scripts are not found
        """
        # CLI scripts should be in the same directory as this module
        current_dir = os.path.dirname(__file__)
        
        # Verify CLI scripts exist
        cli_scripts = [
            "process_manager_server_cli.py",
            "root_controller_server_cli.py", 
            "child_controller_server_cli.py"
        ]
        
        for script in cli_scripts:
            script_path = os.path.join(current_dir, script)
            if not os.path.exists(script_path):
                raise FileNotFoundError(f"CLI script not found: {script_path}")
                
        return current_dir
        
    def _build_python_command(self, cli_script: str, **kwargs) -> str:
        """
        Build the Python CLI execution command.
        
        Args:
            cli_script: Name of the CLI script to execute
            **kwargs: Command-line arguments for the CLI script
            
        Returns:
            Python command string for remote execution
        """
        script_path = os.path.join(self.cli_script_path, cli_script)
        
        cmd_parts = [
            f"cd {self.working_directory}",
            f"{self.python_executable} {script_path}"
        ]
        
        # Add CLI arguments
        cli_args = []
        for key, value in kwargs.items():
            if value is not None:
                # Convert underscores to hyphens for CLI arguments
                cli_key = key.replace('_', '-')
                cli_args.append(f"--{cli_key} {value}")
        
        # Combine base command with arguments
        python_command = " && ".join(cmd_parts)
        if cli_args:
            python_command += " " + " ".join(cli_args)
            
        return python_command
        
    def _build_remote_shell_command(self, python_command: str) -> str:
        """
        Build complete remote shell command with environment setup.
        
        Args:
            python_command: The Python command to execute
            
        Returns:
            Complete shell command with environment setup
        """
        # Add environment variable exports
        env_exports = []
        for k, v in self.env_vars.items():
            env_exports.append(f"export {k}={v}")

        # Build command sequence
        command_parts = [self.env_setup_script]
        command_parts.extend(env_exports)
        command_parts.append(python_command)
        
        # Join with && to ensure each step must succeed
        return " && ".join(command_parts)
        
    def _build_ssh_command(self, host: str, remote_command: str) -> List[str]:
        """
        Build complete SSH command arguments list.
        
        Args:
            host: Target host for SSH connection
            remote_command: Command to execute remotely
            
        Returns:
            Complete SSH command as argument list ready for execution
        """
        # Determine user@host format
        if '@' in host:
            user_host = host
        else:
            user_host = f"{self.default_user}@{host}"
            
        # Build SSH command
        ssh_args = ["/usr/bin/ssh", user_host, "-o", "StrictHostKeyChecking=no"]
        
        # Add host key check disable options if requested
        if self.disable_host_key_check:
            ssh_args.extend([
                "-o", "LogLevel=error",
                "-o", "GlobalKnownHostsFile=/dev/null",
                "-o", "UserKnownHostsFile=/dev/null"
            ])
            
        # Add custom SSH options
        for option in self.ssh_options:
            ssh_args.extend(["-o", option])
            
        # Add remote command execution
        ssh_args.extend(["bash", "-c", remote_command])
        
        return ssh_args
        
    def build_manager_server_command(self, 
                                   server_id: str,
                                   port: int, 
                                   max_workers: int,
                                   log_file: str,
                                   host_index: int = 0) -> BootServerCommand:
        """
        Build complete SSH command to start a Manager gRPC server.
        
        Args:
            server_id: Unique identifier for the server instance
            port: TCP port for the server to bind to
            max_workers: Maximum number of worker threads
            log_file: Path to log file for server output
            host_index: Index of host to use from available hosts
            
        Returns:
            BootServerCommand with complete SSH command ready for execution
        """
        # Build Python command
        python_cmd = self._build_python_command(
            "process_manager_server_cli.py",
            port=port,
            workers=max_workers,
            log_file=log_file
        )
        
        # Build complete remote command
        remote_cmd = self._build_remote_shell_command(python_cmd)
        
        # Select host
        host = self.hosts[host_index % len(self.hosts)]
        
        # Build complete SSH command
        ssh_command = self._build_ssh_command(host, remote_cmd)
        
        return BootServerCommand(
            server_id=server_id,
            complete_ssh_command=ssh_command,
            description=f"Manager server {server_id} on port {port} via {host}",
            expected_port=port
        )
        
    def build_root_controller_server_command(self,
                                           server_id: str,
                                           port: int,
                                           max_workers: int,
                                           log_file: str,
                                           manager_port: int,
                                           host_index: int = 0) -> BootServerCommand:
        """
        Build complete SSH command to start a RootController gRPC server.
        
        Args:
            server_id: Unique identifier for the server instance
            port: TCP port for the server to bind to
            max_workers: Maximum number of worker threads
            log_file: Path to log file for server output
            manager_port: Port of the Manager server to connect to
            host_index: Index of host to use from available hosts
            
        Returns:
            BootServerCommand with complete SSH command ready for execution
        """
        # Build Python command
        python_cmd = self._build_python_command(
            "root_controller_server_cli.py",
            port=port,
            workers=max_workers,
            log_file=log_file,
            manager_port=manager_port
        )
        
        # Build complete remote command
        remote_cmd = self._build_remote_shell_command(python_cmd)
        
        # Select host
        host = self.hosts[host_index % len(self.hosts)]
        
        # Build complete SSH command
        ssh_command = self._build_ssh_command(host, remote_cmd)
        
        return BootServerCommand(
            server_id=server_id,
            complete_ssh_command=ssh_command,
            description=f"RootController server {server_id} on port {port}, connecting to Manager on port {manager_port} via {host}",
            expected_port=port
        )
        
    def build_child_controller_server_command(self,
                                            server_id: str,
                                            port: int,
                                            max_workers: int,
                                            log_file: str,
                                            root_port: int,
                                            child_name: str,
                                            host_index: int = 0) -> BootServerCommand:
        """
        Build complete SSH command to start a ChildController gRPC server.
        
        Args:
            server_id: Unique identifier for the server instance
            port: TCP port for the server to bind to
            max_workers: Maximum number of worker threads
            log_file: Path to log file for server output
            root_port: Port of the RootController server to connect to
            child_name: Unique name identifier for this child controller
            host_index: Index of host to use from available hosts
            
        Returns:
            BootServerCommand with complete SSH command ready for execution
        """
        # Build Python command
        python_cmd = self._build_python_command(
            "child_controller_server_cli.py",
            port=port,
            workers=max_workers,
            log_file=log_file,
            root_port=root_port,
            child_name=child_name
        )
        
        # Build complete remote command
        remote_cmd = self._build_remote_shell_command(python_cmd)
        
        # Select host
        host = self.hosts[host_index % len(self.hosts)]
        
        # Build complete SSH command
        ssh_command = self._build_ssh_command(host, remote_cmd)
        
        return BootServerCommand(
            server_id=server_id,
            complete_ssh_command=ssh_command,
            description=f"ChildController server {server_id} ({child_name}) on port {port}, connecting to RootController on port {root_port} via {host}",
            expected_port=port
        )