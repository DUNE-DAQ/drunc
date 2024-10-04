import logging
from rich.theme import Theme
from enum import Enum
from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.exceptions import DruncSetupException

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
CONSOLE_THEMES = Theme({
    "info": "dim cyan",
    "warning": "magenta",
    "danger": "bold red"
})
log_levels = {
    'CRITICAL': logging.CRITICAL,
    'ERROR'   : logging.ERROR,
    'WARNING' : logging.WARNING,
    'INFO'    : logging.INFO,
    'DEBUG'   : logging.DEBUG,
    'NOTSET'  : logging.NOTSET,
}

def get_random_string(length):
    import random
    import string
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

def regex_match(regex, string):
    import re
    return re.match(regex, string) is not None

log_level = logging.INFO

def print_traceback():
    from rich.console import Console
    c = Console()
    import os
    try:
        width = os.get_terminal_size()[0]
    except:
        width = 300
    c.print_exception(width=width)


def update_log_level(loglevel):
    log_level = log_levels[loglevel]
    #logging.basicConfig(level=log_level)
    # Update log level for root logger
    logger = logging.getLogger('drunc')
    logger.setLevel(log_level)
    for handler in logger.handlers:
        handler.setLevel(log_level)

    # And then manually tweak 'sh.command' logger. Sigh.
    import sh
    sh_command_level = log_level if log_level > logging.INFO else (log_level+10)
    sh_command_logger = logging.getLogger(sh.__name__)
    sh_command_logger.setLevel(sh_command_level)
    for handler in sh_command_logger.handlers:
        handler.setLevel(sh_command_level)

    # And kafka
    import kafka
    kafka_command_level = log_level if log_level > logging.INFO else (log_level+10)
    kafka_command_logger = logging.getLogger(kafka.__name__)
    kafka_command_logger.setLevel(kafka_command_level)
    for handler in kafka_command_logger.handlers:
        handler.setLevel(kafka_command_level)

    from rich.logging import RichHandler
    from rich.console import Console
    import os
    try:
        width = os.get_terminal_size()[0]
    except:
        width = 150

    logging.basicConfig(
        level=log_level,
        format="%(filename)s:%(lineno)i\t%(name)s:\t%(message)s",
        datefmt="[%X]",
        handlers=[
            #logging.StreamHandler(),
            RichHandler(
                console=Console(width=width),
                rich_tracebacks=False,
                show_path=False,
                tracebacks_width=width
            ) # Make this True, and everything crashes on exceptions (no clue why)
        ]
    )

def get_new_port():
    import socket
    from contextlib import closing
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]

def now_str(posix_friendly=False):
    from datetime import datetime
    if not posix_friendly:
        return datetime.now().strftime("%m/%d/%Y,%H:%M:%S")
    else:
        return datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

def run_coroutine(f):
    from functools import wraps
    import asyncio
    @wraps(f)
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()

        ret = None
        import signal

        main_task = asyncio.ensure_future(f(*args, **kwargs))
        wanna_catch_during_command = [signal.SIGINT]

        for sig in wanna_catch_during_command:
            loop.add_signal_handler(sig, main_task.cancel)

        try:
            ret = loop.run_until_complete(main_task)
        except asyncio.exceptions.CancelledError as e:
            print("Command cancelled")
        finally:
            for sig in wanna_catch_during_command:
                loop.remove_signal_handler(sig)

        if ret:
            return ret

    return wrapper

def expand_path(path, turn_to_abs_path=False):
    from os.path import abspath, expanduser, expandvars
    if turn_to_abs_path:
        return abspath(expanduser(expandvars(path)))
    return expanduser(expandvars(path))


def validate_command_facility(ctx, param, value):
    from click import BadParameter
    from urllib.parse import urlparse
    parsed = ''

    try:
        parsed = urlparse(value)
    except Exception as e:
        raise BadParameter(message=str(e), ctx=ctx, param=param)


    if parsed.path or parsed.params or parsed.query or parsed.fragment:
        raise BadParameter(message=f'Command factory for drunc-controller is not understood', ctx=ctx, param=param)

    match parsed.scheme:
        case 'grpc':
            return str(parsed.netloc)
        case _:
            raise BadParameter(message=f'Command factory for drunc-controller only allows \'grpc\'', ctx=ctx, param=param)


def resolve_localhost_and_127_ip_to_network_ip(address):
    from socket import gethostbyname, gethostname
    this_ip = gethostbyname(gethostname())
    if 'localhost' in address:
        address = address.replace('localhost', this_ip)

    import re
    ip_match = re.search(
        "((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)",
        address
    )
    # https://stackoverflow.com/a/25969006

    if ip_match.group(0).startswith('127.'):
        address = address.replace(ip_match.group(0), this_ip)

    return address

def pid_info_str():
    import os
    return f'Parent\'s PID: {os.getppid()} | This PID: {os.getpid()}'

import signal

def ignore_sigint_sighandler():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def parent_death_pact(signal=signal.SIGHUP):
    import ctypes
    import sys
    """
    Commit to kill current process when parent process dies.
    Each time you spawn a new process, run this to set signal
    handler appropriately (e.g put it at the beginning of each
    script, and in multiprocessing startup code).
    """
    assert sys.platform == 'linux', \
        "this fn only works on Linux right now"
    libc = ctypes.CDLL("libc.so.6")
    # see include/uapi/linux/prctl.h in kernel
    PR_SET_PDEATHSIG = 1
    # last three args are unused for PR_SET_PDEATHSIG
    retcode = libc.prctl(PR_SET_PDEATHSIG, signal, 0, 0, 0)
    if retcode != 0:
        raise Exception("prctl() returned nonzero retcode %d" % retcode)

from drunc.exceptions import DruncException
class IncorrectAddress(DruncException):
    pass

def https_or_http_present(address:str):
    if not address.startswith('https://') and not address.startswith('http://'):
        raise IncorrectAddress('Endpoint should start with http:// or https://')


def http_post(address, data, as_json=True, ignore_errors=False, **post_kwargs):
    https_or_http_present(address)

    from requests import post
    if as_json:
        r = post(address, json=data, **post_kwargs)
    else:
        r = post(address, data=data, **post_kwargs)

    if not ignore_errors:
        r.raise_for_status()
    return r

def http_get(address, data, as_json=True, ignore_errors=False, **post_kwargs):
    https_or_http_present(address)

    from requests import get
    log = logging.getLogger("http_get")

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

    from requests import patch
    if as_json:
        r = patch(address, json=data, **post_kwargs)
    else:
        r = patch(address, data=data, **post_kwargs)

    if not ignore_errors:
        r.raise_for_status()
    return r


def http_delete(address, data, as_json=True, ignore_errors=False, **post_kwargs):
    https_or_http_present(address)

    from requests import delete
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


def get_control_type_from_cli(CLAs:list[str]) -> ControlType:
    for CLA in CLAs:
        if   CLA.startswith("rest://"): return ControlType.REST_API
        elif CLA.startswith("grpc://"): return ControlType.gRPC

    raise DruncSetupException("Could not find if the child was controlled by gRPC or a REST API")

def get_control_type_and_uri_from_connectivity_service(
    connectivity_service:ConnectivityServiceClient,
    name:str,
    timeout:int=10, # seconds
    retry_wait:float=0.1, # seconds
    progress_bar:bool=False,
    title:str=None,
) -> tuple[ControlType, str]:

    uris = []
    from drunc.connectivity_service.client import ApplicationLookupUnsuccessful
    logger = logging.getLogger('get_control_type_and_uri_from_connectivity_service')
    import time
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn, TimeElapsedColumn

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TimeRemainingColumn(),
        TimeElapsedColumn()
    ) as progress:

        task = progress.add_task(title, total=timeout, visible=progress_bar)
        start = time.time()

        while time.time() - start < timeout:
            progress.update(task, completed=time.time() - start)

            try:
                uris = connectivity_service.resolve(name+'_control', 'RunControlMessage')
                if len(uris) == 0:
                    raise ApplicationLookupUnsuccessful
                else:
                    break

            except ApplicationLookupUnsuccessful as e:
                el = time.time() - start
                logger.debug(f"Could not resolve \'{name}_control\' elapsed {el:.2f}s/{timeout}s")
                time.sleep(retry_wait)



    if len(uris) != 1:
        raise ApplicationLookupUnsuccessful(f"Could not resolve the URI for \'{name}_control\' in the connectivity service, got response {uris}")

    uri = uris[0]['uri']

    return get_control_type_from_cli([uri]), uri