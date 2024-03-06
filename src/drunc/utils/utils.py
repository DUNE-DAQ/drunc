import logging
from rich.theme import Theme

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
        width = 150
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
    import os
    try:
        width = os.get_terminal_size()[1]
    except:
        width = 150

    logging.basicConfig(
        level=log_level,
        format="\"%(name)s\": %(message)s",
        datefmt="[%X]",
        handlers=[
            #logging.StreamHandler(),
            RichHandler(rich_tracebacks=False, tracebacks_width=width) # Make this True, and everything crashes on exceptions (no clue why)
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
        return asyncio.get_event_loop().run_until_complete(f(*args, **kwargs))

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
            return parsed.netloc
        case _:
            raise BadParameter(message=f'Command factory for drunc-controller only allows \'grpc\'', ctx=ctx, param=param)

