"""
Script to check the power status (on/off) of the NP0x readout hardware.

For WIBs, each device gets pinged. If reachable, attempt to query its FEMB power status.

Including the AMCs is planned for the future.
"""

import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from datetime import datetime

import pytz
from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.live import Live
from rich.table import Table


# --- DYNAMIC PATH SETUP ---
def setup_wib_path():
    """
    Sets up the path to the local copy of the dune-wib-firmware repository if it exists.

    Searches for the dune-wib-firmware source directory in the work area root and in
    sourcecode. If the directory is found, it is added to sys.path for dynamic imports.
    Returns the path if found, else None.

    Args:
        None

    Returns:
        str or None: The path to the dune-wib-firmware/sw directory if found, else None.

    Raises:
        EnvironmentError: If DBT_AREA_ROOT is not set in the environment.
    """
    # Define the root of this work area to start the search from
    work_area_root = os.getenv("DBT_AREA_ROOT", None)
    if not work_area_root:
        raise EnvironmentError(
            "DBT_AREA_ROOT environment variable not set, ensure you are running from a "
            "DUNE DAQ release."
        )

    # Define potential search paths
    search_paths = [
        work_area_root,
        os.path.join(work_area_root, "sourcecode"),
    ]

    # Search for the dune-wib-firmware/sw directory in the defined paths
    for base_source in search_paths:
        potential_path = os.path.join(base_source, "dune-wib-firmware/sw")
        if os.path.isdir(potential_path):
            if potential_path not in sys.path:
                sys.path.insert(0, potential_path)  # TODO: Is this needed?
            return potential_path
    return None


# Get the path to the WIB firmware/software interface if it exists, and attempt to
# import the WIB library if found. If the library is not available, the script will
# still run but will only show online/offline status without FEMB details.
WIB_FW_SW_IFACE_PATH = setup_wib_path()
WIB_LIB_AVAILABLE = False
WIB = None
wibpb = None

if WIB_FW_SW_IFACE_PATH:
    try:
        import wib_pb2 as wibpb
        from wib import WIB

        WIB_LIB_AVAILABLE = True
    except (ImportError, ModuleNotFoundError):
        pass

# Define the addresses of the hardware to check. This is structured as:
# {
#   "Apparatus Name": {
#       "Apparatus Resource/Asset/Name TBC": {
#           "WIB Number": "IP Address",
#           ...
#       },
#       ...
#   },
#   ...
# }
WIB_DATA = {
    "NP02 CB": {
        "NP02 CB": {
            "601": "10.73.137.50",
            "602": "10.73.137.51",
            "603": "10.73.137.52",
            "604": "10.73.137.53",
            "605": "10.73.137.54",
            "606": "10.73.137.122",
        }
    },
    "NP02": {
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
    },
    "NP04 CB": {
        "NP04 CB": {
            "001": "10.73.137.20",
            "002": "10.73.137.21",
            "003": "10.73.137.22",
            "004": "10.73.137.23",
            "005": "10.73.137.24",
        }
    },
    "NP04": {
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
    },
}


def check_hardware(ip: str) -> dict:
    """
    Checks the hardware status of a given IP address by first pinging it to determine if
    it is online, and if it is online, attempting to query its FEMB power status using
    the WIB library.

    Args:
        ip (str): The IP address of the hardware to check.

    Returns:
        dict: A dictionary containing the online status and FEMB power status of the
        hardware, with the following structure:
            {
                "online": bool,  # True if the device is reachable, False otherwise
                "fembs": list[bool]  # A list of 4 booleans indicating the power status
                    of each FEMB (True for powered, False for not powered)
            }

    Raises:
        None: All exceptions are caught and handled within the function, with the final
            status defaulting to "offline" and all FEMBs as "not powered" in case of any
            errors (e.g. timeouts, subprocess errors, gRPC errors, etc.).
    """
    # Default state is 'In Progress' (None)
    final_status = {"online": False, "fembs": [False] * 4}

    try:
        # 1. Ping
        cmd = ["ping", "-c", "1", "-W", "1", ip]
        ping_res = subprocess.run(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=1.5
        )

        if ping_res.returncode == 0:
            final_status["online"] = True

            # 2. Protocol Check
            if WIB_LIB_AVAILABLE:
                try:
                    wib_inst = WIB(ip)
                    req = wibpb.GetFEMBStatus()
                    rep = wibpb.GetFEMBStatus.FEMBStatus()
                    # Run command in a short-lived worker to enforce a hard timeout
                    with ThreadPoolExecutor(max_workers=1) as grpc_exec:
                        grpc_future = grpc_exec.submit(wib_inst.send_command, req, rep)
                        grpc_future.result(timeout=2.0)
                    if hasattr(rep, "femb_power") and len(rep.femb_power) == 4:
                        final_status["fembs"] = list(rep.femb_power)
                    else:
                        final_status["fembs"] = [False] * 4
                except (TimeoutError, Exception):
                    final_status["fembs"] = [False] * 4
            else:
                final_status["fembs"] = [False] * 4
        else:
            final_status["online"] = False
            final_status["fembs"] = [False] * 4

    except Exception:
        # Catch-all for timeouts or subprocess errors
        pass

    return final_status


def make_wib_table(category: str, wibs: dict[str, str], results_map: dict) -> Table:
    """
    Creates a Rich Table for a given category of WIBs, showing their online status and
    FEMB power status.

    Args:
        category (str): The name of the category (e.g. "NP02 CB", "CRP4", etc.) to be
            displayed as the table title.
        wibs (dict): A mapping of WIB numbers to their corresponding IP addresses for
            this category.
        results_map (dict): A mapping of IP addresses to their hardware status results,
            where each result is a dictionary containing 'online' status and 'fembs'
            status.

    Returns:
        Table: A Rich Table object representing the status of the WIBs in this category,
            with the WIB number, online status, and FEMB power status displayed in a
            visually intuitive format (e.g. green checkmarks for online/powered, red Xs
            for offline/unpowered, and dimmed dots for unknown status).

    Raises:
        None
    """

    # Create a table with a title based on the category name.
    table = Table(title=f"[magenta]{category}[/]", box=box.ROUNDED, border_style="dim")

    # Add columns for WIB number and each of the 4 FEMBs, with centered text. The WIB
    # number will be colored based on online status, and the FEMB columns will show
    # icons based on their power status.
    table.add_column("WIB #", justify="center")
    for i in range(4):
        table.add_column(f"FEMB {i}", justify="center")

    # Iterate through the WIBs in this category, adding a row for each. The WIB number
    # is styled based on whether it's online (green) or offline (red), and the FEMB
    # columns show a green checkmark if powered, a red X if not powered, or dimmed dots
    # if the status is unknown (e.g. if the ping check hasn't completed yet). The
    # results_map is used to get the current status for each IP, and if no result is
    # available yet, the row will show the WIB number in white and the FEMB columns as
    # dimmed dots to indicate that the status is still being checked.
    for wib_num, ip in wibs.items():
        # Get the result for this IP from the results_map.
        res = results_map.get(ip)

        # If no result is available yet (res is None), show the WIB number in white and
        # the FEMB columns as dimmed dots. Otherwise, style the WIB number based on
        # online status and show the FEMB columns with green checkmarks or red Xs based
        # on their power status.
        if res is None:
            wib_text = f"[white]{wib_num}[/]"
            femb_icons = ["[dim]...[/]"] * 4
        else:
            wib_style = "bold green" if res["online"] else "bold red"
            wib_text = f"[{wib_style}]{wib_num}[/]"

            femb_icons = []
            for state in res["fembs"]:
                if state is True:
                    femb_icons.append("[bold green]✔[/]")
                else:
                    femb_icons.append("[bold red]✘[/]")

        table.add_row(wib_text, *femb_icons)
    return table


def generate_display(results_map: dict) -> Table:
    """
    Generates the overall display grid for the current results.

    This function creates a grid layout using the Rich library, where each apparatus
    group (e.g. "NP02 CB", "NP02", "NP04 CB", "NP04") is displayed in its own section.
    For each group, it iterates through the subcategories and creates a table for each
    using the make_wib_table function. The resulting tables are arranged in columns
    within the grid. The display is designed to be updated live as results come in,
    showing the current status of each WIB and its FEMBs based on the results_map
    provided.

    Args:
        results_map (dict): A mapping of IP addresses to their hardware status results,
            where each result is a dictionary containing 'online' status and 'fembs'
            status.

    Returns:
        Table: A Rich Table object representing the current status display for all
            hardware.

    Raises:
        None
    """

    # Create a main grid to hold all apparatus groups. The grid is set to expand to fill
    # the available space. Each group will be added as a row, with its own set of tables
    # for the subcategories.
    grid = Table.grid(expand=True)
    for group_name, sub_cats in WIB_DATA.items():
        grid.add_row(f"\n[bold cyan]{group_name}[/]")
        tables = [
            make_wib_table(cat, wibs, results_map) for cat, wibs in sub_cats.items()
        ]
        grid.add_row(Columns(tables, equal=True, expand=False))
    return grid


def main():
    """
    Prints the power status of the hardware defined in WIB_DATA.

    For each WIB, the script first checks if it's online via ping, then if it is online,
    the FEMB power status is queried using the WIB library. Results are displayed in a
    live-updating table format.
    """

    # Set up the singular console to which all output is redirected.
    console = Console()

    # Print header with timestamp
    now = datetime.now(pytz.UTC)
    console.print(
        "\n[bold cyan]Checking NP0x hardware status at "
        f"{now.strftime('%Y-%m-%d %H:%M:%S %Z')}[/]\n"
    )

    # Get all the IP addresses from the WIB_DATA structure to check. This flattens the
    # nested structure into a single list of IPs.
    all_ips = [
        ip
        for apparatus in WIB_DATA.values()
        for resource in apparatus.values()
        for ip in resource.values()
    ]

    # Initialize results map as empty/None for all IPs to force dots initially
    results = {ip: None for ip in all_ips}

    # Define a ThreadPoolExecutor to check hardware in parallel, and a mapping of
    # futures to IPs
    executor = ThreadPoolExecutor(max_workers=10)

    # Submit all hardware checks to the executor and store the future-to-IP mapping for
    # later reference
    futures = {executor.submit(check_hardware, ip): ip for ip in all_ips}

    # If the WIB library is not available, print a warning message to the user. This is
    # done after the main display loop to ensure it doesn't interfere with the
    # live-updating tables. The message provides guidance on how to resolve the issue if
    # the library is not found, or informs the user that the WIB firmware repository is
    # not present if that's the case. The warning is styled to stand out and is enclosed
    # in horizontal lines for emphasis.
    if not WIB_LIB_AVAILABLE:
        console.print("-" * 40)
        console.print("[bold yellow]Hardware Communication Warning:[/]")
        if WIB_FW_SW_IFACE_PATH:
            console.print(
                f"Modules found but not loaded. Try running [red]pip install zmq[/] and [red]make -o build/%.d python[/] in:\n[blue]{WIB_FW_SW_IFACE_PATH}[/]"
            )
        else:
            console.print(
                "Couldnt check wib status. [italic]dune-wib-firmware[/italic] repo not found."
            )
        console.print("-" * 40)

    # Use a Live context to update the display as results come in. As each future
    # completes, the corresponding IP's result is updated in the results map, and the
    # display is refreshed to show the new status. A final update is done after all
    # futures complete to ensure the display is fully up to date. The try-except block
    # allows for graceful interruption with Ctrl+C, ensuring the executor is shut down
    # properly.
    try:
        with Live(
            generate_display(results), console=console, refresh_per_second=5
        ) as live:
            for future in as_completed(futures):
                ip = futures[future]
                results[ip] = future.result()
                live.update(generate_display(results))

            # Final sweep
            live.update(generate_display(results))

    except KeyboardInterrupt:
        pass
    finally:
        executor.shutdown(wait=False)

    # Print final status summary and any warnings about hardware communication if the
    # WIB library is not available.
    console.print("\n[bold green]Scan complete.[/]")


if __name__ == "__main__":
    main()
