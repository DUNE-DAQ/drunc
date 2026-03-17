import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

import paramiko
from rich import box
from rich.console import Console
from rich.live import Live
from rich.table import Table

# List of NP0x cluster hosts
NP0X_CLUSTER_HOSTS = sorted(
    [
        "np02-srv-001",
        "np02-srv-002",
        "np02-srv-003",
        "np02-srv-004",
        "np02-srv-005",
        "np04-srv-001",
        "np04-srv-002",
        "np04-srv-003",
        "np04-srv-004",
        "np04-srv-005",
        "np04-srv-011",
        "np04-srv-012",
        "np04-srv-013",
        "np04-srv-014",
        "np04-srv-015",
        "np04-srv-016",
        "np04-srv-017",
        "np04-srv-018",
        "np04-srv-019",
        "np04-srv-021",
        "np04-srv-022",
        "np04-srv-024",
        "np04-srv-026",
        "np04-srv-028",
        "np04-srv-029",
        "np04-srv-030",
        "np04-srv-031",
    ]
)

VALID_MARK: str = "[bold green]✔[/]"
INVALID_MARK: str = "[bold red]✘[/]"


class TrackingAutoAddPolicy(paramiko.MissingHostKeyPolicy):
    """
    Custom policy to track missing host keys and update the result dict accordingly.
    """

    def __init__(self, result_dict):
        """
        Initialize with a reference to the result dictionary to update key status.
        """
        self.result_dict = result_dict

    def missing_host_key(self, client, hostname: str, key: paramiko.PKey) -> None:
        """
        When a host key is missing, update the result dictionary to indicate that the
        key is being added.
        """
        # Update the result dictionary to reflect the missing key status
        self.result_dict["ssh_key_status"] = "ADD KEY TO KNOWN_HOSTS"
        self.result_dict["ssh_key_color"] = "bold yellow"


def ping_host(hostname: str) -> bool:
    """
    Ping a host to check if it is reachable.

    This function uses the system's ping command to send a single ICMP echo request to
    the specified hostname.

    Args:
        hostname: The hostname or IP address of the host to ping.

    Returns:
        bool: True if the host is reachable (pingable), False otherwise.

    Raises:
        subprocess.CalledProcessError: If the ping command fails to execute properly.
    """
    command = ["ping", "-c", "1", "-W", "1", hostname]
    return (
        subprocess.call(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        == 0
    )


def load_ssh_config() -> paramiko.SSHConfig:
    """
    Load the user's SSH configuration from ~/.ssh/config using Paramiko's SSHConfig
    class.

    This function reads the SSH configuration file and parses it to create an SSHConfig
    object that can be used to look up host-specific settings when connecting to hosts.

    Args:
        None

    Returns:
        paramiko.SSHConfig: An SSHConfig object containing the parsed SSH configuration.

    Raises:
        FileNotFoundError: If the SSH configuration file is not found at the expected
        location.
    """
    # Determine the absolute path to the SSH configuration file.
    config_path = os.path.expanduser("~/.ssh/config")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"SSH config file not found at {config_path}")

    # Create an SSHConfig object and parse the SSH configuration file to populate it
    # with the host-specific settings.
    ssh_config = paramiko.SSHConfig()

    # Open the SSH configuration file and parse it to populate the SSHConfig object.
    if os.path.exists(config_path):
        with open(config_path) as f:
            ssh_config.parse(f)
    return ssh_config


def get_host_info(host_alias: str, ssh_config: paramiko.SSHConfig) -> dict:
    """
    Connect to a host using Paramiko and retrieve its status, key verification status,
    CPU vendor, and uptime information.

    This function attempts to establish an SSH connection to the specified host alias
    using the provided SSH configuration. It checks the host key against known hosts,
    retrieves CPU vendor and uptime information, and handles various exceptions to
    determine the host's status.

    Args:
        host_alias (str): The alias of the host to connect to, as defined in the SSH
            configuration.
        ssh_config (paramiko.SSHConfig): An SSHConfig object containing the parsed
            SSH configuration to use for looking up host-specific settings.

    Returns:
        dict: A dictionary containing the host's alias, connection status, key
        verification status, CPU vendor, uptime, and any additional details. The keys
        in the dictionary include:
            - "alias": The host alias used for the connection.
            - "status": The connection status, which can be "UP", "OFFLINE", or
                "SCANNING".
            - "key_status": The status of the host key verification, which can be
                "Verified", "MISMATCH", or "Not Verified".
            - "key_color": The color code to use for displaying the key status.
            - "vendor": The CPU vendor string retrieved from the host.
            - "cpu_color": The color code to use for displaying the CPU vendor in the
                UI.
            - "uptime": The uptime string retrieved from the host.
            - "details": Additional details about the host, such as CPU model
                information, which is initialized as an empty string and can be
                populated based on the command output.
    Raises:
        paramiko.BadHostKeyException: If the host key does not match the expected key in
            the known hosts file, indicating a potential security issue.
        Exception: Any other exceptions that occur during the connection attempt, which
            will be handled to indicate that the host is offline or the key is not
    """

    # Initialize the result dictionary with default values, assuming the host is offline
    # until proven otherwise.
    result = {
        "alias": host_alias,
        "pingable": False,
        "sshable": False,
        "status": "OFFLINE",
        "ssh_key_status": "Unknown",
        "ssh_key_color": "dim white",
        "cpu_color": "dim white",
        "uptime": "N/A",
        "details": "",
    }

    # Look up the host configuration from the SSH config using the provided host alias.
    host_conf = ssh_config.lookup(host_alias)

    # Determine the real hostname to connect to. If the SSH config provides a "hostname"
    # entry for this alias, use that; otherwise, use the alias itself as the hostname.
    hostname = host_conf.get("hostname", host_alias)

    # First, check if the host is pingable before attempting an SSH connection. If the
    # host is not pingable, attempting the SSH connection can be skipped and it can be
    # marked as offline.
    result["pingable"] = ping_host(hostname)
    if not result["pingable"]:
        result["details"] = "No ICMP Response"
        return result

    # Initialize the SSH client
    client = paramiko.SSHClient()

    # Set the custom missing host key policy to track and update the result dictionary
    client.set_missing_host_key_policy(TrackingAutoAddPolicy(result))

    # Load system host keys to ensure we have the latest known hosts information. If
    # this fails, use the default behavior of the SSH client, which will handle missing
    # keys according to the policy set below.
    try:
        client.load_system_host_keys()
    except Exception:
        pass

    # Look up the host configuration from the SSH config using the provided alias. This
    # will allow us to retrieve the real hostname, username, port, and key file to use
    # for the connection. If the alias is not found in the SSH config, we will use the
    # alias itself as the hostname.
    host_conf = ssh_config.lookup(host_alias)

    # Determine the real hostname to connect to. If the SSH config provides a "hostname"
    # entry for this alias, use that; otherwise, use the alias itself as the hostname.
    hostname = host_conf.get("hostname", host_alias)

    # Attempt to connect to the host using the SSH client. If the connection is
    # successful, execute the command to retrieve CPU and uptime information, parse the
    # output, and update the result dictionary accordingly. If exceptions occur during
    # the connection attempt, handle them to update the result dictionary accordingly.
    try:
        # Prepare the connection arguments based on the SSH config.
        connect_args = {
            "hostname": hostname,
            "username": host_conf.get("user", os.getlogin()),
            "port": int(host_conf.get("port", 22)),
            "timeout": 5,
            "key_filename": host_conf.get("identityfile", None),
        }

        # Attempt to establish an SSH connection to the host using the prepared
        # arguments. If the host key is missing, the custom policy will handle it and
        # update the result dict.
        client.connect(**connect_args)
        result["sshable"] = True
        result["status"] = "UP"

        if result["ssh_key_status"] == "Unknown":
            result["ssh_key_status"] = "Verified"
            result["ssh_key_color"] = "green"

        # Parse the command output to extract CPU vendor, model details, and uptime.
        _, stdout, _ = client.exec_command("lscpu && uptime -p")
        cmd_output = stdout.read().decode().strip()

        for line in cmd_output.splitlines():
            if line.startswith("up "):
                result["uptime"] = line.replace("up ", "")
            if "Vendor ID:" in line:
                v = line.split(":")[1].strip()
                if "AuthenticAMD" in v:
                    result["cpu_color"] = "bold red"
                elif "GenuineIntel" in v:
                    result["cpu_color"] = "bold blue"
            if "Model name:" in line:
                result["details"] = line.split(":")[1].strip()

    # Handle the case where the host key does not match the expected key in the known
    # hosts file. This indicates a potential security issue, and we will update the
    # result dictionary to reflect that the key is a mismatch and the host is down.
    except paramiko.BadHostKeyException:
        result["ssh_key_status"] = "MISMATCH"
        result["ssh_key_color"] = "bold red"
        result["details"] = "Host Key Changed!"

    # Handle authentication failures, which indicate that the host is offline or the key
    # is not valid for this host.
    except paramiko.AuthenticationException:
        result["ssh_key_status"] = "Verified"
        result["ssh_key_color"] = "green"
        result["details"] = "Auth Failed"

    # Handle SSH exceptions, which can occur for various reasons such as network issues,
    # SSH service not running on the host, or other SSH-related problems.
    except paramiko.SSHException as e:
        result["status"] = "OFFLINE"
        result["details"] = f"SSH Error: {str(e)[:20]}"

    # Handle any other exceptions that occur during the connection attempt. Treat this
    # as an indication that the host is offline or unreachable.
    except Exception:
        result["details"] = "Conn Error"
    finally:
        client.close()

    return result


def generate_table(results_map: dict[str, str]) -> Table:
    """
    Generate a Rich Table object to display the status of the NP0x cluster hosts.

    This function creates a Rich Table with columns for Host, Status, User SSH Key
    Status, CPU Model/Details, and Uptime. It iterates through results_map, and updates
    the table rows based on the its content.

    Args:
        results_map (dict[str, str]): A dictionary mapping host aliases to their status
            information, including connection status, key verification status, CPU
            details, and uptime.

    Returns:
        Table: A Rich Table object populated with the status information for each host
        in the NP0x cluster, ready to be rendered in the console.
    """

    # QOL feature
    up_count: int = sum(1 for res in results_map.values() if res["status"] == "UP")
    total_hosts: int = len(results_map)

    # Create a Rich Table with appropriate columns and styling to display the host
    # status information.
    table = Table(
        title=f"ProtoDUNE Cluster [bold cyan]({up_count}/{total_hosts} Online)[/]",
        box=box.ROUNDED,
    )

    # Define the columns for the table.
    table.add_column("Host", justify="center", style="magenta", no_wrap=True)
    table.add_column("Ping", justify="center")
    table.add_column("SSH", justify="center")
    table.add_column("Key Status", justify="center")
    table.add_column("CPU Details", justify="center")
    table.add_column("Uptime", justify="center")

    # Iterate through the results_map and add a row to the table for each host.
    for host in NP0X_CLUSTER_HOSTS:  # type: str
        res: dict[str, str] = results_map.get(host)

        ping_mark: str = VALID_MARK if res["pingable"] else INVALID_MARK
        ssh_mark: str = VALID_MARK if res["sshable"] else INVALID_MARK
        key_status: str = f"[{res['ssh_key_color']}]{res['ssh_key_status']}[/]"
        uptime_display: str = f"[dim white]{res['uptime']}[/]"

        # RESTORED: Wrapping the details in the specific cpu_color
        details_colored = f"[{res['cpu_color']}]{res['details']}[/]"

        table.add_row(
            res["alias"],
            ping_mark,
            ssh_mark,
            key_status,
            details_colored,
            uptime_display,
        )
    return table


def main():
    """
    Main function to execute the NP0x cluster status check and display results in a
    live-updating table.

    This function initializes the console and SSH configuration, sets up a results map
    to track the status of each host, and uses a ThreadPoolExecutor to concurrently
    check the status of each host in the NP0x cluster. The results are displayed in a
    live-updating table using Rich's Live feature, which refreshes the display as
    results come in.

    Args:
        None

    Returns:
        None

    Raises:
        Any exceptions that occur during the execution of the host checks will be
        handled within the get_host_info function.
    """

    # Initialize the console for Rich output and load the SSH configuration.
    console = Console()
    console.print("")  # Buffer line for better aesthetics
    ssh_config = load_ssh_config()

    # Define the default results map with initial values for each host. This map will be
    # updated as results come in from the concurrent checks.
    results_map: dict[str, dict[str, str]] = {
        host: {
            "alias": host,
            "pingable": False,
            "sshable": False,
            "status": "WAITING",
            "ssh_key_status": "Pending",
            "ssh_key_color": "dim white",
            "cpu_color": "dim white",
            "uptime": "...",
            "details": "Scanning...",
        }
        for host in NP0X_CLUSTER_HOSTS
    }

    # Use Rich's Live to create a live-updating table. The table will be refreshed as
    # results come in from the concurrent checks.
    with Live(
        generate_table(results_map), console=console, refresh_per_second=10
    ) as live:
        with ThreadPoolExecutor(max_workers=15) as executor:
            # Map each host to a future that will execute the get_host_info function
            # concurrently.
            futures = {
                executor.submit(get_host_info, h, ssh_config): h
                for h in NP0X_CLUSTER_HOSTS
            }

            # As each future completes, update the results map with the new information
            # and refresh the live table to reflect the updated status of the hosts.
            for future in as_completed(futures):
                host: str = futures[future]
                results_map[host] = future.result()
                live.update(generate_table(results_map))

    console.print("\n[bold green]Scan Complete.[/]")


if __name__ == "__main__":
    main()
