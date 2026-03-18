"""
Pings all the WIBs in the NP0x cryostats and displays their status (online vs offline).

Currently only does WIBs, will be updated to FEMBs, ongoing discussions are in place
with Roger.

Will do AMC crates and AMCs too, but later.
"""

import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pytz
from daqpytools.logging.formatter import timezone_name as tz
from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.live import Live
from rich.table import Table

# Data Mapping
WIB_DATA = {
    "NP04 CB": {
        "001": "10.73.137.20",
        "002": "10.73.137.21",
        "003": "10.73.137.22",
        "004": "10.73.137.23",
        "005": "10.73.137.24",
    },
    "APA1": {
        "101": "10.73.137.26",
        "102": "10.73.137.27",
        "103": "10.73.137.28",
        "104": "10.73.137.29",
        "105": "10.73.137.30",
    },
    "APA2": {
        "201": "10.73.137.31",
        "202": "10.73.137.32",
        "203": "10.73.137.33",
        "204": "10.73.137.34",
        "205": "10.73.137.35",
    },
    "APA3": {
        "301": "10.73.137.36",
        "302": "10.73.137.37",
        "303": "10.73.137.38",
        "304": "10.73.137.39",
        "305": "10.73.137.40",
    },
    "APA4": {
        "401": "10.73.137.41",
        "402": "10.73.137.42",
        "403": "10.73.137.43",
        "404": "10.73.137.44",
        "405": "10.73.137.45",
    },
    "NP02 CB": {
        "601": "10.73.137.50",
        "602": "10.73.137.51",
        "603": "10.73.137.52",
        "604": "10.73.137.53",
        "605": "10.73.137.54",
        "606": "10.73.137.122",
    },
    "CRP4": {
        "1001": "10.73.137.126",
        "1002": "10.73.137.127",
        "1003": "10.73.137.128",
        "1004": "10.73.137.137",
        "1005": "10.73.137.129",
        "1006": "10.73.137.130",
    },
    "CRP5": {
        "1101": "10.73.137.131",
        "1102": "10.73.137.132",
        "1103": "10.73.137.133",
        "1104": "10.73.137.134",
        "1105": "10.73.137.135",
        "1106": "10.73.137.136",
    },
}


def ping_host(ip: str) -> bool:
    """
    Pings a given IP address to check if it's online.

    Any exceptions that occur during the ping process will be caught and result in a
    False return value.

    Args:
        ip: The IP address to ping

    Returns:
        True if the host is online (ping successful), False otherwise

    Raises:
        None
    """

    # Construct the ping command. Ping once with a timeout of 1 second.
    cmd = ["ping", "-c", "1", "-W", "1", ip]

    # Execute the ping command, suppressing output and errors. Use a timeout to avoid
    # hanging on unresponsive hosts.
    try:
        result = subprocess.run(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=1.5
        )
        return result.returncode == 0
    except Exception:
        return False


def make_wib_table(
    category: str, wibs: dict[str, str], status_map: dict[str, bool | None]
) -> Table:
    """
    Creates a Rich Table for a given category of WIBs, showing their online status and
    address.

    Args:
        category: The name of the category (e.g., "NP04 CB", "APA1", etc.). This is an
            entry from the WIB_DATA mapping and will be used as the table title.
        wibs: A mapping of WIB numbers to their IP addresses for this category
        status_map: A mapping of IP addresses to their online status (True/False/None)

    Returns:
        A Rich Table object representing the WIBs in this category and their status

    Raises:
        None
    """

    # Create a Rich Table with a title based on the category.
    table = Table(title=f"[magenta]{category}[/]", box=box.ROUNDED, border_style="dim")

    # Add columns for WIB number, address, and online status.
    table.add_column("WIB #", justify="center")
    table.add_column("Address", style="dim white")
    table.add_column("Online?", justify="center")

    # For each relevant WIB, show the status.
    for wib_num, ip in wibs.items():
        # Look up the status of this IP in the status map.
        res = status_map.get(ip)

        # WIB is online
        if res is True:
            status = "[bold green]✔[/]"

        # WIB is offline
        elif res is False:
            status = "[bold red]✘[/]"

        # Status is unknown (ping not completed yet)
        else:
            status = "[yellow]...[/]"

        # Add a row to the table for this WIB with its number, IP address, and status.
        table.add_row(wib_num, ip, status)

    return table


def generate_table(status_map: dict[str, bool | None]) -> Columns:
    """
    Generates a Rich Columns object containing tables for each category of WIBs.

    Args:
        status_map: A mapping of IP addresses to their online status

    Returns:
        A Rich Columns object containing the WIB status tables for display

    Raises:
        None
    """

    tables: list[Table] = [
        make_wib_table(cat, wibs, status_map) for cat, wibs in WIB_DATA.items()
    ]
    return Columns(tables, equal=True, expand=False)


def main() -> None:
    """
    Main function to execute the NP0x HW status check and display results in a
    live-updating table.

    This function initializes the console, parses out all the IP addresses, and pings
    them concurrently while updating the display in real-time. It handles graceful
    shutdown on keyboard interrupt and ensures that all threads are properly terminated.

    Args:
        None

    Returns:
        None

    Raises:
        Any exceptions that occur during the execution of the host checks will be
        handled within the get_host_info function.
    """

    # Initialize the console for Rich output
    console = Console()

    # Get the timezone-aware current time for display in the header.
    now = datetime.now(pytz.timezone(tz))

    # Print a blank line for spacing and a header to indicate the start of the scan
    console.print("\n")
    console.print(
        f"[bold cyan]Checking NP0x hardware status at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}[/]\n"
    )

    # Extract all IPs from the WIB_DATA for processing
    all_ips: list[str] = [ip for cat in WIB_DATA.values() for ip in cat.values()]

    # Initialize a results map to store the status of each IP, starting with None (unknown)
    results_map: dict[str, bool | None] = {ip: None for ip in all_ips}

    # 1. Create executor outside a context manager to allow manual shutdown control
    executor = ThreadPoolExecutor(max_workers=40)
    futures = {executor.submit(ping_host, ip): ip for ip in all_ips}

    # Use Rich's Live to create a live-updating table. The table will be refreshed as
    # results come in from the concurrent checks.
    try:
        with Live(
            generate_table(results_map), console=console, auto_refresh=True
        ) as live:
            # While we have commands executing, we wait for them to complete. Use a
            # timeout to periodically refresh the table and check for completed futures.
            while futures:
                try:
                    # Update the table with results as they come in. Remove completed
                    # futures from the tracking dict to avoid re-processing.
                    for future in as_completed(futures, timeout=0.1):
                        ip = futures.pop(future)
                        results_map[ip] = future.result()
                        live.update(generate_table(results_map))

                # No futures have completed, but refresh the table to keep it "live"
                except TimeoutError:
                    live.update(generate_table(results_map))

                # If all futures are done, break out of the loop to finish up
                if not futures:
                    break

    # Handle graceful shutdown on keyboard interrupt. This allows the user to stop the
    # scan early without leaving hanging threads.
    except KeyboardInterrupt:
        pass

    # When exiting, all threads are properly shut down.
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    # Final display of results after all checks are complete.
    console.print("\n[bold green]Scan complete.[/]")


if __name__ == "__main__":
    main()
