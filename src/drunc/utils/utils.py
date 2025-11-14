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
from daqpytools.logging.logger import get_daq_logger, setup_root_logger
from requests import delete, get, patch, post
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
    # This sets up the utils handler as well, which is used in many cases
    # Works for now, but preferrably this should not be here
    # See #691
    setup_standard_loggers()
    return setup_root_logger("drunc", log_level)


def get_logger(logger_name: str, *args, **kwargs):
    """Returns / constructs default logging instances. Prepends all loggers with 'drunc'
    to inherit from the root 'drunc' logger.
    Wraps to the daqpytools implementation, see for more details

    Args:
        logger_name (str): Name of the logger
        args, kwargs: Passed without modification to the daqpytools implementation
    """
    return get_daq_logger(f"drunc.{logger_name}", *args, **kwargs)


def setup_standard_loggers() -> None:
    get_logger(logger_name="utils", rich_handler=True)


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

    ip_match: re.Match = re.search(
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


def get_control_type_and_uri_from_cli(CLAs: list[str]) -> ControlType:
    for CLA in CLAs:
        if CLA.startswith("rest://"):
            return ControlType.REST_API, resolve_localhost_and_127_ip_to_network_ip(
                CLA.replace("rest://", "")
            )
        elif CLA.startswith("grpc://"):
            return ControlType.gRPC, resolve_localhost_and_127_ip_to_network_ip(
                CLA.replace("grpc://", "")
            )
    raise DruncSetupException(
        "Could not find if the child was controlled by gRPC or a REST API"
    )


def get_control_type_and_uri_from_connectivity_service(
    connectivity_service,
    name: str,
    timeout: int = 10,  # seconds
    retry_wait: float = 0.1,  # seconds
    progress_bar: bool = False,
    title: str = None,
) -> tuple[ControlType, str]:
    uris = []
    logger = get_logger("utils.get_control_type_and_uri_from_connectivity_service")

    start = time.time()
    elapsed = 0

    if progress_bar:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeRemainingColumn(),
            TimeElapsedColumn(),
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
