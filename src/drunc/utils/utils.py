"""A set of utility functions for drunc."""

import ctypes
import ipaddress
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
from typing import Protocol, cast
from urllib.parse import ParseResult, urlparse

from click import BadParameter, Context, Parameter
from daqpytools.logging import get_daq_logger, setup_root_logger
from requests import Response, delete, get, patch, post
from rich.console import Console
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
    """Set up the base logger which all other loggers will inherit.

    This base logger is named the 'drunc' logger, and functions similarly to the root
    logger. It should have no handlers attached to it.

    Args:
        log_level (str): Log level for the root logger.

    Returns:
        logging.Logger: Configured drunc root logger instance.

    """
    return setup_root_logger("drunc", log_level)


def get_logger(
    logger_name: str,
    log_level: int | str = logging.NOTSET,
    use_parent_handlers: bool = True,
    rich_handler: bool = False,
    file_handler_path: str | None = None,
    stream_handlers: bool = False,
    ers_kafka_session: str | None = None,
    throttle: bool = False,
    **extras: object,
) -> logging.Logger:
    """Get a logger instance for the given logger name."""
    return get_daq_logger(
        f"drunc.{logger_name}",
        log_level,
        use_parent_handlers,
        rich_handler,
        file_handler_path,
        stream_handlers,
        ers_kafka_session,
        throttle,
        **extras,
    )


def get_shared_rich_console(logger: logging.Logger) -> Console | None:
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
    """Strip out all the basicConfig handlers from other repositories, which define
    handlers with the root logger.
    """
    root = logging.getLogger()
    if root.handlers:
        root.handlers.clear()


def get_random_string(length: int) -> str:
    """Generate a random string of lowercase ASCII letters.

    Args:
        length (int): The desired length of the random string.

    Returns:
        str: A random string of the specified length.
    """
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


def regex_match(regex: str, string: str) -> bool:
    """Check if a regex pattern matches a string.

    Args:
        regex (str): The regular expression pattern.
        string (str): The string to match against.

    Returns:
        bool: True if the pattern matches, False otherwise.
    """
    return re.match(regex, string) is not None


def get_new_port() -> int:
    """Get an available port number.

    Returns:
        int: An available port number.
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return int(s.getsockname()[1])


def now_str(posix_friendly: bool = False) -> str:
    """Get the current time as a formatted string.

    Args:
        posix_friendly (bool): If True, use POSIX-friendly format. Defaults to False.

    Returns:
        str: The current time as a formatted string.
    """
    if not posix_friendly:
        return datetime.now().strftime("%m/%d/%Y,%H:%M:%S")
    else:
        return datetime.now().strftime("%Y-%m-%d-%H-%M-%S")


def expand_path(path: str, turn_to_abs_path: bool = False) -> str:
    """Expand a path with user and environment variables.

    Args:
        path (str): The path to expand.
        turn_to_abs_path (bool): If True, also convert to absolute path.
            Defaults to False.

    Returns:
        str: The expanded path.
    """
    if turn_to_abs_path:
        return os.path.abspath(os.path.expanduser(os.path.expandvars(path)))
    return os.path.expanduser(os.path.expandvars(path))


def validate_command_facility(
    ctx: Context | None, param: Parameter | None, value: str
) -> str:
    """Validate a command facility parameter.

    Args:
        ctx (Any): Click context.
        param (Any): Click parameter.
        value (str): The value to validate.

    Returns:
        str: The validated netloc.

    Raises:
        BadParameter: If the value is invalid.
    """
    parsed: ParseResult
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
    """Replace 127.x.x.x and 0.x.x.x IPs with the provided hostname.

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


def host_is_local(host: str) -> bool:
    """Check if a host is local.

    Args:
        host (str): The hostname or IP to check.

    Returns:
        bool: True if the host is local, False otherwise.
    """
    if host in [
        "localhost",
        socket.gethostname(),
        socket.gethostbyname(socket.gethostname()),
    ]:
        return True

    if host.startswith("127.") or host.startswith("0."):
        return True

    return False


def pid_info_str() -> str:
    """Get a string with process ID information.

    Returns:
        str: A string containing the parent and current process IDs.
    """
    return f"Parent's PID: {os.getppid()} | This PID: {os.getpid()}"


def ignore_sigint_sighandler() -> None:
    """Ignore SIGINT (Ctrl+C) signals."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def parent_death_pact(signal: int = signal.SIGHUP) -> None:
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


# 777 PERMISSIONS ARE COMPLETELY TEMPORARY
# An established procedure for multi users will need to be discussed with sysadmins
# will be removed when done
def touch_and_chmod(filepath: str, mode=0o777):
    """Makes and sets the permissions of a file.
    This is used to ensure multiuser support when accessing files etc."""

    with open(filepath, "a"):
        os.utime(filepath, None)
    os.chmod(filepath, mode)


class IncorrectAddress(DruncException):
    """Exception raised when an address is invalid."""

    pass


def https_or_http_present(address: str) -> None:
    """Validate that an address starts with http:// or https://.

    Args:
        address (str): The address to validate.

    Raises:
        IncorrectAddress: If the address does not start with http:// or https://.
    """
    if not address.startswith("https://") and not address.startswith("http://"):
        raise IncorrectAddress("Endpoint should start with http:// or https://")


def http_post(
    address: str,
    data: object,
    as_json: bool = True,
    ignore_errors: bool = False,
    **post_kwargs: object,
) -> Response:
    """Send an HTTP POST request.

    Args:
        address (str): The URL to send the request to.
        data (Any): The data to send in the request body.
        as_json (bool): If True, send data as JSON. Defaults to True.
        ignore_errors (bool): If True, do not raise exceptions for HTTP errors. Defaults to False.
        **post_kwargs: Additional keyword arguments to pass to requests.post.

    Returns:
        Response: The response from the server.
    """
    https_or_http_present(address)
    if as_json:
        r = post(address, json=data, **post_kwargs)  # type: ignore[arg-type]
    else:
        r = post(address, data=data, **post_kwargs)  # type: ignore[arg-type]

    if not ignore_errors:
        r.raise_for_status()
    return r


def http_get(
    address: str,
    data: object,
    as_json: bool = True,
    ignore_errors: bool = False,
    **post_kwargs: object,
) -> Response:
    """Send an HTTP GET request.

    Args:
        address (str): The URL to send the request to.
        data (Any): The data to send in the request body.
        as_json (bool): If True, send data as JSON. Defaults to True.
        ignore_errors (bool): If True, do not raise exceptions for HTTP errors. Defaults to False.
        **post_kwargs: Additional keyword arguments to pass to requests.get.

    Returns:
        Response: The response from the server.
    """
    https_or_http_present(address)

    log = get_logger("utils.http_get")

    log.debug(f"GETTING {address} {data}")
    if as_json:
        r = get(address, json=data, **post_kwargs)  # type: ignore[arg-type]
    else:
        r = get(address, data=data, **post_kwargs)  # type: ignore[arg-type]

    log.debug(r.text)
    log.debug(r.status_code)

    if not ignore_errors:
        log.error(r.text)
        r.raise_for_status()
    return r


def http_patch(
    address: str,
    data: object,
    as_json: bool = True,
    ignore_errors: bool = False,
    **post_kwargs: object,
) -> Response:
    """Send an HTTP PATCH request.

    Args:
        address (str): The URL to send the request to.
        data (Any): The data to send in the request body.
        as_json (bool): If True, send data as JSON. Defaults to True.
        ignore_errors (bool): If True, do not raise exceptions for HTTP errors. Defaults to False.
        **post_kwargs: Additional keyword arguments to pass to requests.patch.

    Returns:
        Response: The response from the server.
    """
    https_or_http_present(address)

    if as_json:
        r = patch(address, json=data, **post_kwargs)  # type: ignore[arg-type]
    else:
        r = patch(address, data=data, **post_kwargs)  # type: ignore[arg-type]

    if not ignore_errors:
        r.raise_for_status()
    return r


def http_delete(
    address: str,
    data: object,
    as_json: bool = True,
    ignore_errors: bool = False,
    **post_kwargs: object,
) -> None:
    """Send an HTTP DELETE request.

    Args:
        address (str): The URL to send the request to.
        data (Any): The data to send in the request body.
        as_json (bool): If True, send data as JSON. Defaults to True.
        ignore_errors (bool): If True, do not raise exceptions for HTTP errors. Defaults to False.
        **post_kwargs: Additional keyword arguments to pass to requests.delete.
    """
    https_or_http_present(address)

    if as_json:
        r = delete(address, json=data, **post_kwargs)  # type: ignore[arg-type]
    else:
        r = delete(address, data=data, **post_kwargs)  # type: ignore[arg-type]

    if not ignore_errors:
        r.raise_for_status()


class _ConnectivityService(Protocol):
    def resolve(self, name: str, message_type: str) -> list[dict[str, object]]: ...


class ControlType(Enum):
    """Enumeration of control types for DUNE DAQ services."""

    Unknown = 0
    gRPC = 1
    REST_API = 2
    Direct = 3


def get_control_type_and_uri_from_cli(cli_args: list[str]) -> tuple[ControlType, str]:
    """Extract control type and URI from CLI arguments.

    Args:
        cli_args (list[str]): The CLI arguments to parse.

    Returns:
        tuple[ControlType, str]: A tuple of (control_type, uri).

    Raises:
        DruncSetupException: If protocol is not 'grpc://' or 'rest://'.
    """
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
    connectivity_service: _ConnectivityService,
    name: str,
    timeout: int = 10,  # seconds
    retry_wait: float = 0.1,  # seconds
    progress_bar: bool = False,
    title: str | None = None,
) -> tuple[ControlType, str]:
    """Get control type and URI from connectivity service.

    Args:
        connectivity_service (object): The connectivity service instance.
        name (str): The name of the service to resolve.
        timeout (int): Maximum time to wait for resolution in seconds. Defaults to 10.
        retry_wait (float): Time to wait between retries in seconds. Defaults to 0.1.
        progress_bar (bool): Whether to display a progress bar. Defaults to False.
        title (str | None): Title for the progress bar. Defaults to None.

    Returns:
        tuple[ControlType, str]: A tuple of (control_type, uri).

    Raises:
        ApplicationLookupUnsuccessful: If the URI cannot be resolved.
    """
    uris: list[dict[str, object]] = []
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

    uri = cast(str, uris[0]["uri"])

    return get_control_type_and_uri_from_cli([uri])


def print_with_timestamp(message: str) -> None:
    """Print a message with a timestamp.

    Args:
        message (str): The message to print.
    """
    now = datetime.now()
    now_str = now.isoformat()
    print(f"{now_str}: {message}")


def format_name_for_cli(name: str) -> str:
    """Format a command name or argument name to be CLI-friendly by replacing
    underscores with hyphens and converting to lowercase.

    Args:
        name (str): The original command name.

    Returns:
        str: The formatted command name suitable for CLI usage.
    """
    return name.replace("_", "-").lower()


def resolve_target_ip(host: str) -> str | None:
    """Intelligently resolve a host to its IP address.

    If host is 'localhost' or '127.0.0.1', it finds the actual LAN IP.

    Args:
        host (str): The name of the host to resolve to LAN IP.

    Returns:
        str: LAN IP of the host.
        None: If the host could not be resolved, None is returned.
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
                return str(s.getsockname()[0])
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


def resolve_context_peer(peer: str) -> str:
    """Resolve a transport-qualified peer string to a display-friendly address.

    The input is expected to look like ``transport:address``. If the address contains
    an IP literal, it is reverse-resolved where possible and IPv6 addresses are
    re-wrapped in brackets.

    Example:
        ``ipv4:10.73.136.70:41750`` -> ``np04-srv-028.cern.ch:41750``

    Args:
        peer (str): Transport-qualified peer string.

    Returns:
        str: The original peer string, or a resolved ``host:port`` representation.
    """

    if not peer:
        return peer

    # Some callers pass a plain host:port string without a transport prefix.
    # Handle those directly instead of assuming the first token is always a transport.
    if peer.startswith("[") or peer.count(":") == 1:
        parsed = _parse_host_port(peer)
        if parsed is not None:
            host, port = parsed
            resolved_host = _resolve_host(host)
            return f"{resolved_host}:{port}"

    match = re.match(r"^(?P<transport>[^:]+):(?P<address>.+)$", peer)
    if not match:
        return peer

    parsed = _parse_host_port(match.group("address"))
    if parsed is None:
        return peer

    host, port = parsed
    resolved_host = _resolve_host(host)
    return f"{resolved_host}:{port}"


def _parse_host_port(address: str) -> tuple[str, str] | None:
    """Extract a host and port from a peer address string.

    Supports bracketed IPv6 addresses such as ``[::1]:1234`` and unbracketed
    ``host:port`` or ``ipv4:port`` forms.

    Args:
        address (str): Address portion of a transport-qualified peer string.

    Returns:
        tuple[str, str] | None: ``(host, port)`` when parsing succeeds, otherwise
        ``None``.
    """

    bracket_match = re.match(r"^\[(?P<host>[^\]]+)\]:(?P<port>\d+)$", address)
    if bracket_match:
        return bracket_match.group("host"), bracket_match.group("port")

    host, sep, port = address.rpartition(":")
    if sep and port.isdigit():
        return host, port

    return None


def _resolve_host(host: str) -> str:
    """Reverse-resolve an IP host and keep IPv6 output bracketed.

    Args:
        host (str): Hostname or IP literal to resolve.

    Returns:
        str: The resolved hostname, or the original host if resolution fails.
    """

    try:
        ip_obj = ipaddress.ip_address(host)
    except ValueError:
        return host

    try:
        resolved_host, _, _ = socket.gethostbyaddr(str(ip_obj))
    except (socket.herror, socket.gaierror, socket.timeout, OSError):
        resolved_host = host

    if ":" in resolved_host and not resolved_host.startswith("["):
        return f"[{resolved_host}]"
    return resolved_host


def is_port_available(host: str, port: int, timeout: int = 2) -> bool:
    """Check if the given port number on a specified host is available.

    Args:
        host (str): The host name to check.
        port (int): The port number to check.
        timeout (int): Timeout of attempting to establish the connection. Defaults to 2.

    Returns:
        bool: True if the port is available, False otherwise.
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
    """Check if a file is read-only.

    Args:
        file_path (str): Path of file to check.

    Returns:
        bool: True if the file is read-only, False otherwise.
    """
    return not os.access(file_path, os.W_OK)
