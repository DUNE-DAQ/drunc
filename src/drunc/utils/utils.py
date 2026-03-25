import ctypes
import logging
import os
import random
import re
import signal
import socket
import string
import sys
import time
from contextlib import closing
from datetime import datetime
from enum import Enum
from urllib.parse import urlparse

from click import BadParameter
from daqpytools.logging import get_daq_logger, setup_root_logger
from requests import delete, get, patch, post
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.theme import Theme

from drunc.connectivity_service.exceptions import ApplicationLookupUnsuccessful
from drunc.exceptions import DruncException, DruncSetupException

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
CONSOLE_THEMES = Theme({"info": "dim cyan", "warning": "magenta", "danger": "bold red"})


def get_root_logger(log_level: str) -> logging.Logger:
    """
    Set up the base logger which all other loggers will inherit.
    This base logger is named the 'drunc' logger, and functions similarly to the root
    logger. It should have no handlers attached to it.

    Args:
        log_level (str): Log level for the root logger.

    Returns:
        logging.Logger: Configured drunc root logger instance.

    """
    return setup_root_logger("drunc", log_level)


def get_logger(logger_name: str, *args, **kwargs) -> logging.Logger:
    """Returns / constructs default logging instances. Prepends all loggers with 'drunc'
    to inherit from the root 'drunc' logger.
    Wraps to the daqpytools implementation, see for more details

    Args:
        logger_name (str): Name of the logger
        args, kwargs: Passed without modification to the daqpytools implementation
    """
    return get_daq_logger(f"drunc.{logger_name}", *args, **kwargs)


def get_shared_rich_console(logger: logging.Logger):
    """
    Traverses logger hierarchy to find a FormattedRichHandler's console.

    Using the same rich.Console object is necessary for ensuring that rich tables and
    log messages are printed in the same order and don't interleave. If no RichHandler
    is found, returns None.

    Args:
        logger: The logger to start searching from.

    Returns:
        The rich.Console object if found, otherwise None.

    Raises:
        None
    """
    # Get the current logger
    current = logger

    # Iterate through this logger and its parents to find a RichHandler
    while current:
        # Iterate through the handlers of the current logger
        for handler in current.handlers:
            # If the handler is an instance of RichHandler, if so return its console
            if isinstance(handler, RichHandler):
                return handler.console

        # If propagate is False or there is no parent logger, stop searching
        if not current.propagate or current.parent is None:
            break

        # Move up to the parent logger
        current = current.parent

    # No RichHandler found in the logger hierarchy, return None
    return None


def strip_non_drunc_loggers() -> None:
    """
    Strip out all the basicConfig handlers from other repositories, which define
    handlers with the root logger.
    """
    root = logging.getLogger()
    if root.handlers:
        root.handlers.clear()


def get_random_string(length):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


def regex_match(regex, string):
    return re.match(regex, string) is not None


def get_new_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def now_str(posix_friendly=False):
    if not posix_friendly:
        return datetime.now().strftime("%m/%d/%Y,%H:%M:%S")
    else:
        return datetime.now().strftime("%Y-%m-%d-%H-%M-%S")


def expand_path(path, turn_to_abs_path=False):
    if turn_to_abs_path:
        return os.path.abspath(os.path.expanduser(os.path.expandvars(path)))
    return os.path.expanduser(os.path.expandvars(path))


def validate_command_facility(ctx, param, value):
    parsed = ""
    try:
        parsed = urlparse(value)
    except Exception as e:
        raise BadParameter(message=str(e), ctx=ctx, param=param)

    if parsed.path or parsed.params or parsed.query or parsed.fragment:
        raise BadParameter(
            message="Command factory for drunc-controller is not understood",
            ctx=ctx,
            param=param,
        )

    match parsed.scheme:
        case "grpc":
            return str(parsed.netloc)
        case _:
            raise BadParameter(
                message="Command factory for drunc-controller only allows 'grpc'",
                ctx=ctx,
                param=param,
            )


def address_regex(address: str, hostname_or_ip: str) -> str:
    """
    Replace 127.x.x.x and 0.x.x.x IPs with the provided hostname

    This is useful when a service binds to localhost or 127.x.x.x, but we
    want to access it using the hostname or network IP.

    Args:
        address (str): The address to resolve.
        hostname_or_ip (str): The hostname or IP to replace 127.x.x.x or 0.x.x.x with.

    Returns:
        str: The address with 127.x.x.x and 0.x.x.x replaced by the hostname or IP.
    """

    ip_match: re.Match[str] | None = re.search(
        r"((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)",
        address,
    )

    if not ip_match:
        return address

    if ip_match.group(0).startswith("127."):
        address = address.replace(ip_match.group(0), hostname_or_ip)

    if ip_match.group(0).startswith("0."):
        address = address.replace(ip_match.group(0), hostname_or_ip)

    return address


def resolve_localhost_to_hostname(address: str) -> str:
    """
    Replace localhost with the actual hostname of this host.

    This is useful when a service binds to localhost, but we want to access it using the
    hostname.

    Args:
        address (str): The address to resolve.

    Returns:
        str: The address with localhost replaced by the hostname.
    """

    if not address:
        return ""
    hostname: str = socket.gethostname()
    if "localhost" in address:
        address = address.replace("localhost", hostname)

    return address_regex(address, hostname)


def resolve_localhost_and_127_ip_to_network_ip(address: str) -> str:
    """
    Replace localhost and 127.x.x.x IPs with the actual network IP of this host.

    This is useful when a service binds to localhost or 127.x.x.x, but we
    want to access it from another machine on the network.

    Args:
        address (str): The address to resolve.

    Returns:
        str: The address with localhost and 127.x.x.x replaced by the network IP
    """

    this_ip: str = socket.gethostbyname(socket.gethostname())
    if "localhost" in address:
        address = address.replace("localhost", this_ip)

    return address_regex(address, this_ip)


def host_is_local(host):
    if host in [
        "localhost",
        socket.gethostname(),
        socket.gethostbyname(socket.gethostname()),
    ]:
        return True

    if host.startswith("127.") or host.startswith("0."):
        return True

    return False


def pid_info_str():
    return f"Parent's PID: {os.getppid()} | This PID: {os.getpid()}"


def ignore_sigint_sighandler():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def parent_death_pact(signal=signal.SIGHUP):
    """Commit to kill current process when parent process dies.
    Each time you spawn a new process, run this to set signal
    handler appropriately (e.g put it at the beginning of each
    script, and in multiprocessing startup code).
    """
    assert sys.platform == "linux", "this fn only works on Linux right now"
    libc = ctypes.CDLL("libc.so.6")
    # see include/uapi/linux/prctl.h in kernel
    PR_SET_PDEATHSIG = 1
    # last three args are unused for PR_SET_PDEATHSIG
    retcode = libc.prctl(PR_SET_PDEATHSIG, signal, 0, 0, 0)
    if retcode != 0:
        raise Exception("prctl() returned nonzero retcode %d" % retcode)


class IncorrectAddress(DruncException):
    pass


def https_or_http_present(address: str):
    if not address.startswith("https://") and not address.startswith("http://"):
        raise IncorrectAddress("Endpoint should start with http:// or https://")


def http_post(address, data, as_json=True, ignore_errors=False, **post_kwargs):
    https_or_http_present(address)
    if as_json:
        r = post(address, json=data, **post_kwargs)
    else:
        r = post(address, data=data, **post_kwargs)

    if not ignore_errors:
        r.raise_for_status()
    return r


def http_get(address, data, as_json=True, ignore_errors=False, **post_kwargs):
    https_or_http_present(address)

    log = get_logger("utils.http_get")

    log.debug(f"GETTING {address} {data}")
    if as_json:
        r = get(address, json=data, **post_kwargs)
    else:
        r = get(address, data=data, **post_kwargs)

    log.debug(r.text)
    log.debug(r.status_code)

    if not ignore_errors:
        log.error(r.text)
        r.raise_for_status()
    return r


def http_patch(address, data, as_json=True, ignore_errors=False, **post_kwargs):
    https_or_http_present(address)

    if as_json:
        r = patch(address, json=data, **post_kwargs)
    else:
        r = patch(address, data=data, **post_kwargs)

    if not ignore_errors:
        r.raise_for_status()
    return r


def http_delete(address, data, as_json=True, ignore_errors=False, **post_kwargs):
    https_or_http_present(address)

    if as_json:
        r = delete(address, json=data, **post_kwargs)
    else:
        r = delete(address, data=data, **post_kwargs)

    if not ignore_errors:
        r.raise_for_status()


class ControlType(Enum):
    Unknown = 0
    gRPC = 1
    REST_API = 2
    Direct = 3


def get_control_type_and_uri_from_cli(cli_args: list[str]) -> tuple[ControlType, str]:
    for arg in cli_args:
        if arg.startswith("rest://"):
            uri = arg.replace("rest://", "")
            uri = resolve_localhost_and_127_ip_to_network_ip(uri)
            return ControlType.REST_API, uri
        elif arg.startswith("grpc://"):
            uri = arg.replace("grpc://", "")
            uri = resolve_localhost_and_127_ip_to_network_ip(uri)
            return ControlType.gRPC, uri
    raise DruncSetupException("Protocol must be 'grpc://' or 'rest://'")


def get_control_type_and_uri_from_connectivity_service(
    connectivity_service,
    name: str,
    timeout: int = 10,  # seconds
    retry_wait: float = 0.1,  # seconds
    progress_bar: bool = False,
    title: str | None = None,
) -> tuple[ControlType, str]:
    uris = []
    logger = get_logger("utils.get_control_type_and_uri_from_connectivity_service")
    shared_console = get_shared_rich_console(logger)

    start = time.time()
    elapsed = 0.0

    if progress_bar:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeRemainingColumn(),
            TimeElapsedColumn(),
            console=shared_console,
            # transient=True,  # Clear the progress bar after completion, once we have established a more complete testing framework with the various failure modes, we can enable this to reduce console clutter
        ) as progress:
            task = progress.add_task(
                f"[yellow]{title}", total=timeout, visible=progress_bar
            )

            while elapsed < timeout:
                progress.update(task, completed=elapsed)

                try:
                    uris = connectivity_service.resolve(
                        name + "_control", "RunControlMessage"
                    )
                    if len(uris) == 0:
                        raise ApplicationLookupUnsuccessful
                    else:
                        break

                except ApplicationLookupUnsuccessful:
                    elapsed = time.time() - start
                    logger.debug(
                        f"Could not resolve '{name}_control' elapsed {elapsed:.2f}s/{timeout}s"
                    )
                    time.sleep(retry_wait)

            progress.update(task, completed=timeout)

    else:
        while elapsed < timeout:
            try:
                uris = connectivity_service.resolve(
                    name + "_control", "RunControlMessage"
                )
                if len(uris) == 0:
                    raise ApplicationLookupUnsuccessful
                else:
                    break

            except ApplicationLookupUnsuccessful:
                elapsed = time.time() - start
                logger.debug(
                    f"Could not resolve '{name}_control' elapsed {elapsed:.2f}s/{timeout}s"
                )
                time.sleep(retry_wait)

    if len(uris) != 1:
        raise ApplicationLookupUnsuccessful(
            f"Could not resolve the URI for '{name}_control' in the connectivity service, got response {uris}"
        )

    uri = uris[0]["uri"]

    return get_control_type_and_uri_from_cli([uri])


def print_with_timestamp(message):
    now = datetime.now()
    now_str = now.isoformat()
    print(f"{now_str}: {message}")


def format_name_for_cli(name: str) -> str:
    """
    Format a command name or argument name to be CLI-friendly by replacing underscores
    with hyphens and converting to lowercase.

    Args:
        name (str): The original command name.

    Returns:
        str: The formatted command name suitable for CLI usage.
    """
    return name.replace("_", "-").lower()


def resolve_target_ip(host: str) -> str | None:
    """
    Intelligently resolves the host.
    If host is 'localhost' or '127.0.0.1', it finds the actual LAN IP.

    Args:
        host - the name of the host to reolve to LAN IP

    Returns:
        str - LAN IP of the host
        None - if the host could not be resolved, None is returned
    """

    log = get_logger("utils.resolve_target_ip")

    # Linux usually resolves the hostname of localhosts to loopback, this needs to be
    # addressed separately
    # if host.lower() in ['localhost', '127.0.0.1', '0.0.0.0', socket.gethostname().lower()]:
    if host_is_local(host.lower()):
        # Need to check external traffic as otherwise resolution goes to loopback or all
        # interface addresses, which fails the check.
        try:
            # Use IPv4 and UDP (no SYN handshake packet, less noisy network)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                # Attempt to connect to the Broadcast Address - this is automatically
                # blocked from sending data outside of the LAN. Use connect - this does
                # send any data, just establishes the connection.
                s.connect(("10.255.255.255", 1))
                return s.getsockname()[0]
        except Exception:
            # Return the loopback address.
            log.warning(f"Failed to resolve the IP address of {host}")
            return "127.0.0.1"

    # Otherwise, resolve the remote hostname normally
    try:
        return socket.gethostbyname(host)
    except socket.gaierror:
        # Server only has an IPv6 address
        log.error(f"Could not resolve host: {host}")
        return None


def is_port_available(host: str, port: int, timeout: int = 2) -> bool:
    """
    Check if the given port number on a specified host is available.

    Args:
        host - the host name to check
        port - the port number to check
        timeout - timeout of attempting to establish the connection

    Returns:
        true - the port is available
        false - the port is not available
    """

    log = get_logger("utils.is_port_available")

    # Resolve the IP address to the LAN IP
    target_ip = resolve_target_ip(host)

    # Cannot resolve hostname, assume unavailable/error
    if not target_ip:
        return False

    log.debug(f"Checking {host} (resolved to {target_ip}) on port {port}")

    # Attempt to connect to the address with IPv4 and TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        try:
            s.connect((target_ip, port))
            # Connection established succesfully - the port is not available
            return False
        except ConnectionRefusedError:
            # Connection failed to establish - the port is available
            return True
        except socket.timeout:
            # Connection timed out - connection may be available, marked as unavailable
            # for safety
            log.debug(
                f"The port {host}:{port} may be available, but connection to the address timed out. Marked as unavailable for safety."
            )
            return False
        except OSError:
            # Catch other network errors
            return False


def file_is_read_only(file_path: str) -> bool:
    """
    Runs checks to see if the file path is read only.

    Args:
        file_path - path of file to read

    Returns:
        bool - true is file is read only, false otherwise
    """
    return not os.access(file_path, os.W_OK)
