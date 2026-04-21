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
        "np04-srv-025",
        "np04-srv-026",
        "np04-srv-028",
        "np04-srv-029",
        "np04-srv-030",
        "np04-srv-031",
    ]
)

# UI Elements
MARK_CHECK = "[bold green]✔[/]"
MARK_CROSS = "[bold red]✘[/]"
STYLE_OFFLINE = "[bold white on #444444] OFFLINE [/]"

# Base template for host tracking
DEFAULT_HOST_DATA = {
    "alias": "",
    "is_pingable": None,  # None: Waiting, True: Up, False: Down
    "ssh_key": "Pending",
    "key_color": "dim white",
    "cpu_count": "-",
    "total_cores": "-",
    "cpu_usage": "-",
    "cpu_model": "Scanning...",
    "model_color": "dim white",
    "ram_total": "-",
    "ram_usage": "-",
    "nfs_ok": False,
    "uptime": "-",
}


class TrackingAutoAddPolicy(paramiko.MissingHostKeyPolicy):
    """
    Custom policy to track missing host keys and update the result dict accordingly.
    """

    def __init__(self, res_dict):
        """
        Initialize with a reference to the result dictionary to update key status.
        """
        self.res_dict = res_dict

    def missing_host_key(self, client, hostname, key):
        """
        When a host key is missing, update the result dictionary to indicate that the
        key is being added.
        """
        # Update the result dictionary to reflect the missing key status
        self.res_dict["ssh_key"] = "ADD KEY"
        self.res_dict["key_color"] = "bold yellow"


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
    try:
        ssh_config.parse(open(config_path))
    except Exception as e:
        raise Exception(f"Error parsing SSH config file: {e}")

    return ssh_config


def ping_host(hostname: str) -> bool:
    """
    Ping a host to check if it is reachable.

    This function uses the system's ping command to send a single ICMP echo request to
    the specified hostname. Sends a single packet and waits for a response for up to 1
    second.

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


def get_host_metrics(host_alias: str, ssh_config: paramiko.SSHConfig) -> dict:
    """
    For a given host alias, perform the following checks:
         - Ping the host to determine if it's online.
         - If pingable, attempt an SSH connection using Paramiko.
         - If SSH is successful, gather CPU, RAM, uptime, and NFS status information.

    Args:
        host_alias: The SSH alias of the host to check.
        ssh_config: The loaded SSH configuration for connection details.

    Returns:
        A dictionary with the results of the checks and gathered metrics for the host.

    Raises:
        Any exceptions during SSH connection or command execution are caught and used
        in the returned dictionary under the 'cpu_model' key for visibility in the UI.
    """

    # Start with a copy of the default host data and set the alias
    result = DEFAULT_HOST_DATA.copy()
    result["alias"] = host_alias

    # Look up the host configuration from the SSH config using the provided host alias.
    host_conf = ssh_config.lookup(host_alias)

    # Determine the real hostname to connect to. If the SSH config provides a "hostname"
    # entry for this alias, use that; otherwise, use the alias itself as the hostname.
    hostname = host_conf.get("hostname", host_alias)

    # First, check if the host is pingable before attempting an SSH connection. If the
    # host is not pingable, attempting the SSH connection can be skipped and it can be
    # marked as offline.
    result["is_pingable"] = ping_host(hostname)
    if not result["is_pingable"]:
        result["details"] = "No ICMP Response"
        return result

    # Initialize the SSH client
    client = paramiko.SSHClient()

    # Set the custom missing host key policy to track and update the result dictionary
    client.set_missing_host_key_policy(TrackingAutoAddPolicy(result))

    # Look up the host configuration from the SSH config using the provided alias. This
    # will allow us to retrieve the real hostname, username, port, and key file to use
    # for the connection. If the alias is not found in the SSH config, we will use the
    # alias itself as the hostname.
    try:
        # Load system host keys to ensure we have the latest known hosts information. If
        # this fails, use the default behavior of the SSH client, which will handle missing
        # keys according to the policy set below.
        client.load_system_host_keys()

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

        # If the connection was successfully established and the key was valid, the
        # connection is verified and can be marked as such.
        if result["ssh_key"] == "Pending":
            result["ssh_key"], result["key_color"] = "Verified", "green"

        # Construct the command to gather the relevant metrics
        cmd = (
            r"lscpu | grep -P '^Socket\(s\):|Vendor ID:|Model name:|Core\(s\) per socket:'; "
            "uptime -p; "
            "top -bn1 | grep 'Cpu(s)' | awk '{print $2 + $4}'; "
            "free -g | awk '/Mem:/ {print $2,$3}'; "
            "[ -d /nfs ] && [ \"$(ls -A /nfs)\" ] && echo 'NFS_OK' || echo 'NFS_MISSING'"
        )

        # Execute the command and read the output
        _, stdout, _ = client.exec_command(cmd)
        lines = stdout.read().decode().strip().splitlines()

        # Define temporary variables to hold CPU socket and core information for
        # calculating total cores
        sockets: int = 0
        cores_per_socket: int = 0

        # Parse the output lines to extract the relevant metrics and update the result
        # dictionary
        for line in lines:
            # 1. Physical CPU Sockets
            if "Socket(s):" in line:
                try:
                    sockets = int(line.split(":")[1].strip())
                    result["cpu_count"] = str(sockets)
                except ValueError:
                    pass

            elif "Core(s) per socket:" in line:
                try:
                    cores_per_socket = int(line.split(":")[1].strip())
                except ValueError:
                    pass

            # 2. CPU Vendor/Color
            elif "Vendor ID:" in line:
                result["model_color"] = "bold red" if "AMD" in line else "bold blue"

            # 3. CPU Model Name
            elif "Model name:" in line:
                result["cpu_model"] = line.split(":")[1].strip()

            # 4. Uptime (looks for the "up" prefix from uptime -p)
            elif line.startswith("up "):
                result["uptime"] = line.replace("up ", "")

            # 5. NFS Status
            elif "NFS_" in line:
                result["nfs_ok"] = line == "NFS_OK"

            # 6. RAM (Looking for the line with two integers: Total Used)
            elif " " in line and "." not in line and not line.startswith("up"):
                parts = line.split()
                if len(parts) == 2:
                    try:
                        total, used = int(parts[0]), int(parts[1])
                        result["ram_total"] = f"{total}GB"
                        result["ram_usage"] = (
                            f"{(used / total) * 100:.1f}%" if total > 0 else "0%"
                        )
                    except ValueError:
                        pass

            # 7. CPU Usage (The only line left that should be a float/decimal)
            else:
                try:
                    # Check if the line is purely a number (CPU percentage)
                    float_val = float(line)
                    result["cpu_usage"] = f"{float_val}%"
                except ValueError:
                    pass

        if sockets > 0 and cores_per_socket > 0:
            result["total_cores"] = str(sockets * cores_per_socket)

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
    n_online = sum(1 for r in results_map.values() if r["is_pingable"] is True)
    n_hosts: int = len(results_map)

    # Create a Rich Table with appropriate columns and styling to display the host
    # status information.
    table = Table(
        title=f"[bold cyan]ProtoDUNE Cluster ({n_online}/{n_hosts} Online)[/]",
        box=box.ROUNDED,
        header_style="bold cyan",
    )

    # Define the columns for the table.
    table.add_column("Host", justify="center", style="magenta")
    table.add_column("Ping Status", justify="center")
    table.add_column("SSH Key", justify="center")
    table.add_column("CPUs", justify="center")
    table.add_column("Hardware / Error Details", justify="center")
    table.add_column("Total cores", justify="center")
    table.add_column("CPU %", justify="center")
    table.add_column("RAM", justify="center")
    table.add_column("RAM %", justify="center")
    table.add_column("NFS mounted", justify="center")
    table.add_column("Uptime", justify="center")

    # Iterate through the results_map and add a row to the table for each host.
    for host in NP0X_CLUSTER_HOSTS:
        # Retrieve the result dictionary for the current host
        r = results_map.get(host)
        hostname = r["alias"] if r else host

        # Host is Pingable
        if r["is_pingable"] is True:
            ping_cell = MARK_CHECK
            key_cell = f"[{r['key_color']}]{r['ssh_key']}[/]"
            nfs_cell = MARK_CHECK if r["nfs_ok"] else MARK_CROSS
            details = f"[{r['model_color']}]{r['cpu_model']}[/]"
            uptime, c_ld, r_ld = (
                f"[dim]{r['uptime']}[/]",
                r["cpu_usage"],
                r["ram_usage"],
            )

        # Scan in Progress
        elif r["is_pingable"] is None:
            ping_cell = "[dim white]WAITING[/]"
            key_cell = nfs_cell = details = uptime = c_ld = r_ld = "-"
            details = "[dim italic]Scanning...[/]"

        # Host is Unreachable (Offline)
        else:
            ping_cell = MARK_CROSS
            key_cell = nfs_cell = uptime = c_ld = r_ld = "-"
            details = f"[bold yellow]{r['cpu_model']}[/]"

        table.add_row(
            hostname,
            ping_cell,
            key_cell,
            r["cpu_count"],
            details,
            r["total_cores"],
            c_ld,
            r["ram_total"],
            r_ld,
            nfs_cell,
            uptime,
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
    results_map = {
        h: DEFAULT_HOST_DATA.copy() | {"alias": h} for h in NP0X_CLUSTER_HOSTS
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
                executor.submit(get_host_metrics, h, ssh_config): h
                for h in NP0X_CLUSTER_HOSTS
            }

            # As each future completes, update the results map with the new information
            # and refresh the live table to reflect the updated status of the hosts.
            for f in as_completed(futures):
                results_map[futures[f]] = f.result()
                live.update(generate_table(results_map))

    console.print("\n[bold green]Scan Complete.[/]")


if __name__ == "__main__":
    main()
