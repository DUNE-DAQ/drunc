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


def regex_match(regex, string):
    import re
    return re.match(regex, string) is not None

log_level = logging.INFO

def update_log_level(loglevel):
    import sh
    log_level = log_levels[loglevel]
    #logging.basicConfig(level=log_level)
    # Update log level for root logger
    logger = logging.getLogger('drunc')
    logger.setLevel(log_level)
    for handler in logger.handlers:
        handler.setLevel(log_level)
    # And then manually tweak 'sh.command' logger. Sigh.
    sh_command_level = log_level if log_level > logging.INFO else (log_level+10)
    sh_command_logger = logging.getLogger(sh.__name__)
    # sh_command_logger.propagate = False
    sh_command_logger.setLevel(sh_command_level)
    for handler in sh_command_logger.handlers:
        handler.setLevel(sh_command_level)

    from rich.logging import RichHandler

    logging.basicConfig(
        level=log_level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            #logging.StreamHandler(),
            RichHandler(rich_tracebacks=False) # Make this True, and everything crashes on exceptions (no clue why)
        ]
    )
# def setup_fancy_logging():
#     from rich.logging import RichHandler




# def get_logger(module_name):
#     global log_level
#     logger = logging.getLogger(module_name)
#     logger.setLevel(log_level)
#     for handler in logger.handlers:
#         handler.setLevel(log_level)
#     return logger#logging.getLogger(module_name)


def get_new_port():
    import socket
    sock = socket.socket()
    sock.bind(('', 0))
    return sock.getsockname()[1]


def now_str():
    from datetime import datetime
    return datetime.now().strftime("%m/%d/%Y,%H:%M:%S.%f")
