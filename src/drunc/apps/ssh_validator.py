"""
Perform a security and connectivity audit of NP0x cluster hosts via SSH.
This script checks for host key mismatches (MITM), new host keys, authentication
failures, and connectivity issues.
It uses the Rich library to display a live-updating table of results in the terminal.
"""

import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

import click
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
MARK_WARN = "[bold yellow]⚠[/]"


def check_host_ssh(host: str) -> dict:
    """
    Audit the SSH connection.

    Executes a native SSH connection to test host key, auth, and MITM status.
    Uses BatchMode to avoid password prompt hangups.

    Args:
        host: The hostname or IP address of the target SSH server.

    Returns:
        A dictionary containing the audit results for the host, including status,
        MITM detection, new key detection, authentication failure, timeout, and details.

    Raises:
        None. All exceptions are caught and logged in the result dictionary.
    """
    # Build the SSH command with options to avoid interactive prompts
    cmd = [
        "ssh",
        "-o",
        "BatchMode=yes",  # Fails instantly instead of asking for a password
        "-o",
        "ConnectTimeout=5",  # 5-second timeout for offline hosts
        "-o",
        "StrictHostKeyChecking=no",  # Bypasses the yes/no prompt, but throws a warning we can parse
        host,
        "exit",
    ]

    # Initialize the result dictionary with default values
    result = {
        "host": host,
        "status": "[dim white]Scanning...[/]",
        "mitm": False,
        "new_key": False,
        "auth_failed": False,
        "timeout": False,
        "details": "",
    }

    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, stdin=subprocess.DEVNULL
        )
        stderr = proc.stderr

        # 1. Check for MITM / Host Key Mismatch
        if "REMOTE HOST IDENTIFICATION HAS CHANGED!" in stderr:
            result["mitm"] = True
            result["status"] = "[bold red]MITM / Key Mismatch[/]"
            result["details"] = "Host key changed! Possible man-in-the-middle."

        # 2. Check if it was an unknown host (Confirmation would normally be required)
        elif "Warning: Permanently added" in stderr:
            result["new_key"] = True

        # 3. Evaluate Process Return Code
        if proc.returncode == 0:
            if result["new_key"]:
                result["status"] = "[bold yellow]OK (Key Auto-Added)[/]"
                result["details"] = "Confirmation was required (New Host)"
            else:
                result["status"] = "[bold green]Connected[/]"
                result["details"] = "Keys verified, Auth successful"
        else:
            if "Permission denied" in stderr:
                result["auth_failed"] = True
                result["status"] = "[bold magenta]Auth Failed[/]"
                if result["new_key"]:
                    result["details"] = "Key added, but publickey auth rejected."
                else:
                    result["details"] = "Check SSH agent or authorized_keys."

            elif any(
                x in stderr.lower()
                for x in [
                    "timed out",
                    "connection refused",
                    "no route",
                    "resolve hostname",
                ]
            ):
                result["timeout"] = True
                result["status"] = "[bold dark_gray]Offline / Unreachable[/]"
                result["details"] = "Host is down or network blocked."

            elif not result["mitm"]:
                result["status"] = "[bold red]SSH Error[/]"
                result["details"] = stderr.strip().split("\n")[0][:45]

    except Exception as e:
        result["status"] = "[bold red]Subprocess Error[/]"
        result["details"] = str(e)[:45]

    return result


def generate_table(results_map: dict) -> Table:
    """
    Generate the Rich Table based on the current state of results_map.

    Args:
        results_map: A dictionary mapping hostnames to their SSH check results.

    Returns:
        A Rich Table object ready for display.

    Raises:
        None
    """
    # Count online hosts for the table title
    n_online = sum(
        1
        for r in results_map.values()
        if "Connected" in r["status"] or "OK" in r["status"]
    )
    n_hosts = len(results_map)

    # Create the Rich Table
    table = Table(
        title=f"[bold cyan]NP0x SSH Security & Connectivity Audit ({n_online}/{n_hosts} Online)[/]",
        box=box.ROUNDED,
        header_style="bold cyan",
    )
    table.add_column("Host", justify="left", style="magenta")
    table.add_column("Connection Status", justify="center")
    table.add_column("MITM Alert", justify="center")
    table.add_column("New Key (Prompt Bypassed)", justify="center")
    table.add_column("Auth Blocked", justify="center")
    table.add_column("Details", justify="left")

    # Populate the table with results
    for host in NP0X_CLUSTER_HOSTS:
        r = results_map.get(
            host,
            {
                "status": "[dim white]Waiting...[/]",
                "mitm": False,
                "new_key": False,
                "auth_failed": False,
                "details": "-",
            },
        )

        mitm_cell = MARK_CROSS if r.get("mitm") else "-"
        new_key_cell = MARK_WARN if r.get("new_key") else "-"
        auth_cell = MARK_CROSS if r.get("auth_failed") else "-"

        table.add_row(
            host, r["status"], mitm_cell, new_key_cell, auth_cell, r["details"]
        )

    return table


@click.command(
    help="""
Perform a security and connectivity audit of NP0x cluster hosts via SSH.

This script checks for host key mismatches (MITM), new host keys, authentication
failures, and connectivity issues. It uses the Rich library to display a 
live-updating table of results in the terminal.
"""
)
def main():
    """
    Run the SSH audit and display the results in a live-updating table.

    Args:
        None

    Returns:
        None

    Raises:
        None. All exceptions are handled within the check_host_ssh function.
    """
    console = Console()
    console.print("")

    # Initialize the results map with placeholder data
    results_map = {
        h: {"status": "[dim white]Waiting...[/]", "details": "-"}
        for h in NP0X_CLUSTER_HOSTS
    }

    # Run the SSH checks concurrently and update the Live table
    with Live(
        generate_table(results_map), console=console, refresh_per_second=10
    ) as live:
        # Use ThreadPoolExecutor to check hosts concurrently
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit SSH check tasks for each host and map futures to hosts
            futures = {
                executor.submit(check_host_ssh, h): h for h in NP0X_CLUSTER_HOSTS
            }

            # As each future completes, update the results map and refresh the live table
            for f in as_completed(futures):
                host = futures[f]
                results_map[host] = f.result()
                live.update(generate_table(results_map))

    console.print("\n[bold green]SSH Audit Complete.[/]")


if __name__ == "__main__":
    main()
