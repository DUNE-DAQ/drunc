import os
from concurrent.futures import ThreadPoolExecutor

import paramiko
from rich import box
from rich.console import Console
from rich.table import Table

NP0X_CLUSTER_HOSTS = [
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
NP0X_CLUSTER_HOSTS = sorted(NP0X_CLUSTER_HOSTS)


class TrackingAutoAddPolicy(paramiko.MissingHostKeyPolicy):
    """
    Custom policy that auto-adds keys (ignoring the 'yes/no' prompt),
    but records that it happened so we can report it in the table.
    """

    def __init__(self, result_dict):
        self.result_dict = result_dict

    def missing_host_key(self, client, hostname, key):
        # Record that we saw a new key
        self.result_dict["key_status"] = "Update known_hosts"
        self.result_dict["key_color"] = "yellow"

        # Perform the standard AutoAdd logic (save to memory/disk)
        client._host_keys.add(hostname, key.get_name(), key)
        if client._host_keys_filename is not None:
            client.save_host_keys(client._host_keys_filename)


def load_ssh_config() -> paramiko.SSHConfig:
    """
    Parses the local user's SSH config file.

    Args:
        None

    Returns:
        paramiko.SSHConfig: An object containing the parsed SSH configuration.

    Notes:
        - This function looks for the SSH config file at ~/.ssh/config.
        - If the file exists, it will be parsed and returned as a paramiko.SSHConfig
            object.
        - If the file does not exist, an empty SSHConfig object will be returned, which
            will cause all hosts to be treated with default settings (hostname = alias,
            port = 22, etc.).
    """
    config_path = os.path.expanduser("~/.ssh/config")
    ssh_config = paramiko.SSHConfig()

    if os.path.exists(config_path):
        with open(config_path) as f:
            ssh_config.parse(f)
    return ssh_config


def get_host_info(host_alias: str, ssh_config: paramiko.SSHConfig) -> dict:
    """
    Connects to a host and retrieves its status and key information.

    Args:
        host_alias (str): The alias of the host as defined in the SSH config.
        ssh_config (paramiko.SSHConfig): The parsed SSH configuration object.

    Returns:
        dict: A dictionary containing the host's alias, real hostname, connection
            status, key status, and uptime details.

    Raises:
        None: All exceptions are caught and handled within the function, with results
            returned in the result dictionary.
    """

    client = paramiko.SSHClient()

    # Default result structure
    result = {
        "alias": host_alias,
        "real_host": "Resolving...",
        "status": "DOWN",
        "key_status": "Verified",  # Default assumption
        "key_color": "green",
        "details": "",
    }

    # Load System Host Keys (known_hosts)
    try:
        client.load_system_host_keys()
    except IOError:
        # No known_hosts file exists
        pass

    # Attach a Custom Policy that tracks new keys
    client.set_missing_host_key_policy(TrackingAutoAddPolicy(result))

    # Parse the SSH config for this host
    host_conf = ssh_config.lookup(host_alias)
    real_host = host_conf.get("hostname", host_alias)
    result["real_host"] = real_host

    try:
        # Define connection parameters for this SSH config
        connect_args = {
            "hostname": real_host,
            "username": host_conf.get("user"),
            "port": int(host_conf.get("port", 22)),
            "timeout": 10,
            "key_filename": host_conf.get("identityfile"),
        }

        # Connect to the host
        client.connect(**connect_args)

        # If a connection is established, get the host uptime
        stdin, stdout, stderr = client.exec_command("uptime -p")
        output = stdout.read().decode().strip()

        # If we got here, the connection is successful, so we mark it as UP
        result["status"] = "UP"
        result["details"] = output if output else "Shell OK (No output)"

        # Close the connection after we're done
        client.close()

    # Address the case where the host key is known but does not match (potential
    # security issue)
    except paramiko.BadHostKeyException:
        result["key_status"] = "MISMATCH"
        result["key_color"] = "bold red"
        result["details"] = "Security Warning: Key Changed"

    # Handle authentication failures (wrong password or key)
    except paramiko.AuthenticationException:
        result["details"] = "Auth Failed (Pass/Key)"

    # Handle SSH protocol errors (e.g., connection issues, timeouts, etc.)
    except paramiko.SSHException as e:
        result["details"] = f"Proto Error: {str(e)}"

    # Handle any other exceptions (like network errors, DNS resolution failures, etc.)
    except Exception:
        result["details"] = "Unreachable"

    return result


def main():
    """
    Main function to check the status of all hosts in the NP0X_CLUSTER_HOSTS list and
    display the results in a formatted table.

    Args:
        None

    Returns:
        None: The function prints the results to the console.

    Notes:
        - The function uses a ThreadPoolExecutor to check multiple hosts concurrently
            for faster results.
        - The results include the connection status (UP/DOWN), key status (Verified,
            Update known_hosts, MISMATCH), and any relevant details (like uptime or
            error messages).
        - The output is displayed in a rich-formatted table for better readability.
    """

    # Initialize the console
    console = Console()

    # Load the user SSH configuration
    ssh_config: paramiko.SSHConfig = load_ssh_config()

    # Insert a clear line for better readability
    console.print("")

    # Create Table with the new column
    table = Table(title="Connection Status", box=box.ROUNDED)
    table.add_column("Host", style="cyan", justify="center")
    table.add_column("Status", justify="center")
    table.add_column("User Key Status", justify="center")
    table.add_column("Uptime details", style="dim white")

    # Check all hosts concurrently
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(get_host_info, host, ssh_config)
            for host in NP0X_CLUSTER_HOSTS
        ]
        for future in futures:
            results.append(future.result())

    # Process results and add rows to the table
    for res in results:
        # Format Status with color based on UP/DOWN
        if res["status"] == "UP":
            status_str = "[bold green]ONLINE[/bold green]"
        else:
            status_str = "[bold red]OFFLINE[/bold red]"

        # Format Key Status using the color determined in the function
        key_str = f"[{res['key_color']}]{res['key_status']}[/{res['key_color']}]"

        table.add_row(res["alias"], status_str, key_str, str(res["details"]))

    # Display the table
    console.print(table)

    # Insert a clear line for better readability
    console.print("")


if __name__ == "__main__":
    main()
