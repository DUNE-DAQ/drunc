#!/usr/bin/env python3
"""
SSH Server Manager Diagnostic Script with Environment Script Support

This script performs detailed diagnostics to identify issues with SSH-based
gRPC server execution using a user-provided environment setup script.
"""

import os
import sys
import time
import tempfile
import subprocess
import socket
from typing import Dict


class SSHDiagnosticWithEnv:
    """
    Comprehensive SSH diagnostic tool for gRPC server manager with environment script.
    
    Performs step-by-step validation of SSH connectivity, environment script execution,
    Python environment setup, and remote execution to identify failure points.
    """
    
    def __init__(self, env_setup_script: str):
        """
        Initialise diagnostic tool with environment setup script.
        
        Args:
            env_setup_script: Path to the shell script that sets up the Python environment
        """
        self.env_setup_script = env_setup_script
        self.results = {}
        self.temp_dir = None
        self.log_file = None

        
        # Configuration matching the failing test
        self.user = os.getenv('USER', 'unknown')
        self.host = 'localhost'
        self.user_host = f"{self.user}@{self.host}"
        self.test_port = 50080
        self.working_dir = os.getcwd()
        self.python_executable = "python3"
        
        # Paths from the failing test
        self.drunc_src_path = os.path.join(self.working_dir, "src")
        self.cli_script_dir = os.path.join(self.drunc_src_path, "drunc", "tests", "grpc")
        self.cli_script = os.path.join(self.cli_script_dir, "process_manager_server_cli.py")
        
        # SSH options from the test
        self.ssh_options = [
            "-o", "StrictHostKeyChecking=no",
            "-o", "LogLevel=error", 
            "-o", "GlobalKnownHostsFile=/dev/null",
            "-o", "UserKnownHostsFile=/dev/null"
        ]
        
    def setup_logging(self) -> None:
        """Set up detailed logging for diagnostic output."""
        self.temp_dir = tempfile.mkdtemp(prefix="ssh_diagnostic_")
        self.log_file = os.path.join(self.temp_dir, "diagnostic.log")
        
        print(f"=== SSH Server Manager Diagnostic Tool (Environment Script) ===")
        print(f"Environment setup script: {self.env_setup_script}")
        print(f"Diagnostic log directory: {self.temp_dir}")
        print(f"Diagnostic log file: {self.log_file}")
        print(f"Target user@host: {self.user_host}")
        print(f"Working directory: {self.working_dir}")
        print(f"Python executable: {self.python_executable}")
        print(f"CLI script path: {self.cli_script}")
        print()
    
    def log_result(self, test_name: str, success: bool, details: str = "", error: str = "") -> None:
        """
        Log test result with detailed information.
        
        Args:
            test_name: Name of the test being performed
            success: Whether the test passed
            details: Additional details about the test
            error: Error message if test failed
        """
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status}: {test_name}")
        
        if details:
            for line in details.split('\n'):
                if line.strip():
                    print(f"    {line}")
        
        if error:
            print(f"    ERROR: {error}")
            
        print()
        
        self.results[test_name] = {
            'success': success,
            'details': details,
            'error': error
        }
    
    def test_environment_script_local(self) -> bool:
        """
        Test the environment setup script locally.
        
        Returns:
            True if environment script works locally, False otherwise
        """
        try:
            # Test sourcing the environment script locally
            test_cmd = f"{self.env_setup_script} && echo 'ENV_SCRIPT_SUCCESS' && which python3 && python3 -c 'import sys; print(\"Python:\", sys.executable)'"
            
            result = subprocess.run(
                ["bash", "-c", test_cmd],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=self.working_dir
            )
            
            success = result.returncode == 0 and "ENV_SCRIPT_SUCCESS" in result.stdout
            
            details = f"Command: {test_cmd}\n"
            details += f"Return code: {result.returncode}\n"
            details += f"Stdout: {result.stdout}\n"
            details += f"Stderr: {result.stderr}"
            
            error = "" if success else "Environment script failed to source or execute properly"
            
            self.log_result(
                "Environment Script (Local)",
                success,
                details,
                error
            )
            
            return success
            
        except subprocess.TimeoutExpired:
            self.log_result(
                "Environment Script (Local)",
                False,
                error="Environment script execution timed out"
            )
            return False
        except Exception as e:
            self.log_result(
                "Environment Script (Local)",
                False,
                error=f"Exception during environment script test: {e}"
            )
            return False
    
    def test_basic_ssh_connectivity(self) -> bool:
        """
        Test basic SSH connectivity to localhost.
        
        Returns:
            True if SSH connection works, False otherwise
        """
        try:
            # Test basic SSH connection with a simple command
            cmd = ["ssh"] + self.ssh_options + [self.user_host, "echo", "SSH_TEST_SUCCESS"]
            
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=10
            )
            
            if result.returncode == 0 and "SSH_TEST_SUCCESS" in result.stdout:
                self.log_result(
                    "Basic SSH Connectivity",
                    True,
                    f"Command: {' '.join(cmd)}\nOutput: {result.stdout.strip()}"
                )
                return True
            else:
                self.log_result(
                    "Basic SSH Connectivity",
                    False,
                    f"Command: {' '.join(cmd)}\nReturn code: {result.returncode}\nStdout: {result.stdout}\nStderr: {result.stderr}",
                    "SSH connection failed"
                )
                return False
                
        except subprocess.TimeoutExpired:
            self.log_result(
                "Basic SSH Connectivity",
                False,
                error="SSH connection timed out"
            )
            return False
        except Exception as e:
            self.log_result(
                "Basic SSH Connectivity",
                False,
                error=f"Exception during SSH test: {e}"
            )
            return False
    
    def test_remote_environment_script(self) -> bool:
        """
        Test environment script execution on remote host via SSH.
        
        Returns:
            True if environment script works remotely, False otherwise
        """
        try:
            # Test environment script sourcing via SSH
            remote_test_cmd = f"{self.env_setup_script} && echo 'REMOTE_ENV_SUCCESS' && which python3 && python3 --version"
            
            ssh_cmd = ["ssh"] + self.ssh_options + [self.user_host, "bash", "-c", f'"{remote_test_cmd}"']
            
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            success = result.returncode == 0 and "REMOTE_ENV_SUCCESS" in result.stdout
            
            details = f"Remote command: {remote_test_cmd}\n"
            details += f"SSH command: {' '.join(ssh_cmd)}\n"
            details += f"Return code: {result.returncode}\n"
            details += f"Stdout: {result.stdout}\n"
            details += f"Stderr: {result.stderr}"
            
            error = "" if success else "Remote environment script execution failed"
            
            self.log_result(
                "Remote Environment Script",
                success,
                details,
                error
            )
            
            return success
            
        except subprocess.TimeoutExpired:
            self.log_result(
                "Remote Environment Script",
                False,
                error="Remote environment script execution timed out"
            )
            return False
        except Exception as e:
            self.log_result(
                "Remote Environment Script",
                False,
                error=f"Exception during remote environment test: {e}"
            )
            return False
    
    def test_remote_python_import(self) -> bool:
        """
        Test Python import capabilities after environment script setup.
        
        Returns:
            True if Python imports work correctly, False otherwise
        """
        try:
            # Test Python import with environment script
            import_test_cmd = f"{self.env_setup_script} && cd {self.working_dir} && python3 -c \"import sys; print('Python path:'); [print(f'  {{p}}') for p in sys.path]; import drunc.tests.grpc.run_grpc_services; print('IMPORT_SUCCESS')\""
            
            ssh_cmd = ["ssh"] + self.ssh_options + [self.user_host, "bash", "-c", f'"{import_test_cmd}"']
            
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            success = result.returncode == 0 and "IMPORT_SUCCESS" in result.stdout
            
            details = f"Import test command: {import_test_cmd}\n"
            details += f"SSH command: {' '.join(ssh_cmd)}\n"
            details += f"Return code: {result.returncode}\n"
            details += f"Stdout: {result.stdout}\n"
            details += f"Stderr: {result.stderr}"
            
            error = "" if success else "Failed to import drunc package after environment setup"
            
            self.log_result(
                "Remote Python Import",
                success,
                details,
                error
            )
            
            return success
            
        except Exception as e:
            self.log_result(
                "Remote Python Import",
                False,
                error=f"Exception during Python import test: {e}"
            )
            return False
    
    def test_file_path_accessibility(self) -> bool:
        """
        Test accessibility of required files and directories.
        
        Returns:
            True if all required paths are accessible, False otherwise
        """
        try:
            # Test file existence locally first
            local_details = []
            local_success = True
            
            paths_to_check = [
                ("Environment script", self.env_setup_script),
                ("Working directory", self.working_dir),
                ("Source directory", self.drunc_src_path),
                ("CLI script directory", self.cli_script_dir),
                ("CLI script file", self.cli_script)
            ]
            
            for name, path in paths_to_check:
                exists = os.path.exists(path)
                local_details.append(f"{name}: {path} {'✓' if exists else '✗'}")
                if not exists:
                    local_success = False
            
            # Test remote file accessibility
            remote_test_cmd = f"test -f {self.env_setup_script} && test -d {self.working_dir} && test -f {self.cli_script} && echo 'REMOTE_FILES_ACCESSIBLE'"
            
            ssh_cmd = ["ssh"] + self.ssh_options + [self.user_host, "bash", "-c", f'"{remote_test_cmd}"']
            
            remote_result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=10
            )
            
            remote_success = remote_result.returncode == 0 and "REMOTE_FILES_ACCESSIBLE" in remote_result.stdout
            
            overall_success = local_success and remote_success
            
            details = "Local file system:\n" + "\n".join(f"  {detail}" for detail in local_details)
            details += f"\n\nRemote accessibility test:\n"
            details += f"  Command: {' '.join(ssh_cmd)}\n"
            details += f"  Return code: {remote_result.returncode}\n"
            details += f"  Output: {remote_result.stdout.strip()}\n"
            details += f"  Error: {remote_result.stderr.strip()}"
            
            error = ""
            if not local_success:
                error += "Required files missing locally. "
            if not remote_success:
                error += "Required files not accessible remotely."
            
            self.log_result(
                "File Path Accessibility",
                overall_success,
                details,
                error.strip()
            )
            
            return overall_success
            
        except Exception as e:
            self.log_result(
                "File Path Accessibility",
                False,
                error=f"Exception during file path test: {e}"
            )
            return False
    
    def test_complete_ssh_command(self) -> bool:
        """
        Test the complete SSH command that would be used by the server manager.
        
        Returns:
            True if command execution succeeds, False otherwise
        """
        try:
            # Build the exact command the SSH server manager would use
            log_file = os.path.join(self.temp_dir, "test_server.log")
            
            # Environment setup + additional exports + CLI command
            remote_cmd_parts = [
                f"{self.env_setup_script}",
                "export GRPC_TRACE=http",
                f"cd {self.working_dir}",
                f"{self.python_executable} {self.cli_script} --port {self.test_port} --workers 2 --log-file {log_file}"
            ]
            
            remote_cmd = " && ".join(remote_cmd_parts)
            
            # Test the complete SSH command
            ssh_cmd = ["ssh"] + self.ssh_options + [self.user_host, "bash", "-c", f'"{remote_cmd}"']
            
            print(f"Testing complete SSH command:")
            print(f"  Remote command: {remote_cmd}")
            print(f"  SSH command: {' '.join(ssh_cmd)}")
            print(f"  Starting server process...")
            
            # Start the server process
            process = subprocess.Popen(
                ssh_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Give server time to start
            print(f"  Waiting 5 seconds for server startup...")
            time.sleep(5)
            
            # Check if port becomes available
            port_available = self.check_port_accessible(self.test_port, timeout=2)
            
            # Get process status
            process_running = process.poll() is None
            
            print(f"  Port {self.test_port} accessible: {port_available}")
            print(f"  Process still running: {process_running}")
            
            # Terminate the server process
            try:
                print(f"  Terminating server process...")
                process.terminate()
                process.wait(timeout=10)
                print(f"  Server process terminated successfully")
            except subprocess.TimeoutExpired:
                print(f"  Force killing server process...")
                process.kill()
                process.wait(timeout=5)
            except Exception as e:
                print(f"  Error terminating process: {e}")
            
            # Get final output
            try:
                stdout, stderr = process.communicate(timeout=2)
            except:
                stdout, stderr = "", ""
            
            # Success if port became available OR process is running without immediate errors
            success = port_available or (process_running and not stderr.strip())
            
            details = f"Remote command: {remote_cmd}\n"
            details += f"SSH command: {' '.join(ssh_cmd)}\n"
            details += f"Process running: {process_running}\n"
            details += f"Port accessible: {port_available}\n"
            details += f"Return code: {process.returncode}\n"
            details += f"Stdout: {stdout[:500]}{'...' if len(stdout) > 500 else ''}\n"
            details += f"Stderr: {stderr[:500]}{'...' if len(stderr) > 500 else ''}"
            
            error = "" if success else "Server failed to start or port did not become accessible"
            
            self.log_result(
                "Complete SSH Command",
                success,
                details,
                error
            )
            
            return success
            
        except Exception as e:
            self.log_result(
                "Complete SSH Command",
                False,
                error=f"Exception during complete SSH command test: {e}"
            )
            return False
    
    def check_port_accessible(self, port: int, timeout: float = 1.0) -> bool:
        """
        Check if a port is accessible for connections.
        
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
    
    def test_port_availability(self) -> bool:
        """
        Test if the target port is available for binding.
        
        Returns:
            True if port is available, False if occupied
        """
        # Check if port is currently in use
        port_in_use = self.check_port_accessible(self.test_port, timeout=0.5)
        
        if port_in_use:
            self.log_result(
                "Port Availability",
                False,
                f"Port {self.test_port} is currently in use",
                f"Port {self.test_port} is occupied - server cannot bind to it"
            )
            return False
        else:
            self.log_result(
                "Port Availability",
                True,
                f"Port {self.test_port} is available for binding"
            )
            return True
    
    def run_full_diagnostic(self) -> Dict[str, bool]:
        """
        Run complete diagnostic suite.
        
        Returns:
            Dictionary mapping test names to success status
        """
        self.setup_logging()
        
        print("Running comprehensive SSH server manager diagnostics with environment script...\n")
        
        # Run all diagnostic tests in order
        tests = [
            ("Port Availability", self.test_port_availability),
            ("Environment Script (Local)", self.test_environment_script_local),
            ("Basic SSH Connectivity", self.test_basic_ssh_connectivity),
            ("Remote Environment Script", self.test_remote_environment_script),
            ("Remote Python Import", self.test_remote_python_import),
            ("File Path Accessibility", self.test_file_path_accessibility),
            ("Complete SSH Command", self.test_complete_ssh_command),
        ]
        
        # Run all tests
        for test_name, test_func in tests:
            print(f"=== {test_name} ===")
            try:
                test_func()
            except Exception as e:
                self.log_result(test_name, False, error=f"Unexpected exception: {e}")
        
        # Summary
        self.print_diagnostic_summary()
        
        return {name: result['success'] for name, result in self.results.items()}
    
    def print_diagnostic_summary(self) -> None:
        """Print comprehensive diagnostic summary with recommendations."""
        print("="*60)
        print("DIAGNOSTIC SUMMARY")
        print("="*60)
        
        passed = sum(1 for result in self.results.values() if result['success'])
        total = len(self.results)
        
        print(f"Tests passed: {passed}/{total}")
        print()
        
        # Show failures and recommendations
        failures = [(name, result) for name, result in self.results.items() if not result['success']]
        
        if failures:
            print("FAILED TESTS AND RECOMMENDATIONS:")
            print("-" * 40)
            
            for test_name, result in failures:
                print(f"✗ {test_name}")
                if result['error']:
                    print(f"  Error: {result['error']}")
                
                # Provide specific recommendations
                if test_name == "Environment Script (Local)":
                    print(f"  Recommendation: Check that {self.env_setup_script} exists and works correctly")
                    print("  Try running it manually")
                elif test_name == "Basic SSH Connectivity":
                    print("  Recommendation: Check SSH service, user permissions, and host key settings")
                elif test_name == "Remote Environment Script":
                    print("  Recommendation: Environment script may not be accessible or executable via SSH")
                    print("  Check file permissions and path accessibility")
                elif test_name == "Remote Python Import":
                    print("  Recommendation: Environment script may not be setting up Python paths correctly")
                    print("  Verify that the environment script sets up drunc package availability")
                elif test_name == "File Path Accessibility":
                    print("  Recommendation: Verify file paths exist and are accessible via SSH")
                elif test_name == "Complete SSH Command":
                    print("  Recommendation: Review command construction and check server logs")
                elif test_name == "Port Availability":
                    print("  Recommendation: Use a different port or kill process using current port")
                
                print()
        else:
            print("✓ All diagnostic tests passed!")
            print("The SSH server manager should work correctly with your environment script.")
        
        print(f"Detailed logs available in: {self.temp_dir}")
    
    def cleanup(self) -> None:
        """Clean up temporary files and resources."""
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                import shutil
                shutil.rmtree(self.temp_dir)
                print(f"Cleaned up diagnostic directory: {self.temp_dir}")
            except Exception as e:
                print(f"Warning: Could not clean up {self.temp_dir}: {e}")


def main():
    """
    Main entry point for SSH diagnostic script with environment setup.
    
    Uses the specified environment script to set up the Python environment
    before running gRPC server diagnostics.
    """
    # Default environment script path 
    default_env_script = "cd /home/aurash/work/09sept && source env.sh"
    
    # Allow override via command line argument
    env_script = sys.argv[1] if len(sys.argv) > 1 else default_env_script
    
    try:
        diagnostic = SSHDiagnosticWithEnv(env_script)
        results = diagnostic.run_full_diagnostic()
        
        # Exit with appropriate code
        all_passed = all(results.values())
        sys.exit(0 if all_passed else 1)
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print(f"Usage: {sys.argv[0]} [environment_script_path]")
        print(f"Default: {default_env_script}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nDiagnostic interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Diagnostic failed with exception: {e}")
        sys.exit(1)
    finally:
        # Uncomment to keep logs for investigation
        # diagnostic.cleanup()
        pass


if __name__ == "__main__":
    main()