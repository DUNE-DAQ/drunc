import asyncio
import logging
import multiprocessing
import os
import signal
import socket
import tempfile
import threading
import time

import click
import psutil
import pytest

from drunc.exceptions import DruncSetupException
from drunc.utils.utils import (
    ControlType,
    IncorrectAddress,
    create_logger_handler,
    expand_path,
    get_control_type_and_uri_from_cli,
    get_logger,
    get_new_port,
    get_random_string,
    host_is_local,
    https_or_http_present,
    now_str,
    parent_death_pact,
    regex_match,
    resolve_localhost_and_127_ip_to_network_ip,
    resolve_localhost_to_hostname,
    run_coroutine,
    setup_root_logger,
    validate_command_facility,
)


def test_get_random_string():
    string = get_random_string(8)

    # Check that the string is a string
    assert isinstance(string, str)

    # Check that the string is of the correct length
    assert len(string) == 8

    # Check that the string is random
    string2 = get_random_string(8)
    assert string != string2


def test_regex_match():
    assert regex_match(".*", "absc")
    assert regex_match(".*", "1234")
    assert regex_match("123.", "1234")


def test_setup_logger(caplog):
    drunc_root_logger = setup_root_logger("DEBUG")
    assert drunc_root_logger.getEffectiveLevel() == logging.DEBUG
    assert get_logger("tester0").getEffectiveLevel() == logging.DEBUG

    drunc_root_logger.setLevel("INFO")
    assert drunc_root_logger.getEffectiveLevel() == logging.INFO
    assert get_logger("tester1").getEffectiveLevel() == logging.INFO

    drunc_root_logger.setLevel("WARNING")
    assert drunc_root_logger.getEffectiveLevel() == logging.WARNING
    assert get_logger("tester2").getEffectiveLevel() == logging.WARNING

    drunc_root_logger.setLevel("ERROR")
    assert drunc_root_logger.getEffectiveLevel() == logging.ERROR
    assert get_logger("tester3").getEffectiveLevel() == logging.ERROR

    drunc_root_logger.setLevel("CRITICAL")
    assert drunc_root_logger.getEffectiveLevel() == logging.CRITICAL
    assert get_logger("tester4").getEffectiveLevel() == logging.CRITICAL

    # Make a temporary file to validate logging to a file
    temp_file = tempfile.NamedTemporaryFile()
    log_path = temp_file.name
    logger = get_logger("tester5")
    create_logger_handler(log_file_path=log_path)
    logger.debug("invisible")
    logger.info("invisible")
    logger.warning("invisible")
    logger.error("invisible")
    logger.critical("VISIBLE")
    good_record = 0
    bad_record = 0
    for record in caplog.records:
        if (
            "VISIBLE" in record.getMessage()
            and record.levelno == logging.CRITICAL
            and "tester5" in record.name
        ):
            good_record += 1
        else:
            bad_record += 1

    assert good_record == 1
    assert bad_record == 0

    with open(log_path) as f:
        temp_file_data = f.read()
        assert "VISIBLE" in temp_file_data
        assert "invisible" not in temp_file_data

    temp_file.close()


def test_get_new_port():
    port = get_new_port()

    # Check that the port is free
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        assert s.connect_ex(("localhost", port)) != 0

    # Check that the port is an integer
    assert isinstance(port, int)

    # Check the range of the port
    assert port > 0
    assert port < 65535


def test_run_coroutine():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    @run_coroutine
    async def test_this_coroutine(val):
        return val

    result = test_this_coroutine("abc")

    assert result == "abc"


@pytest.mark.skip()  # reason="Not implemented correctly"
def test_interrupt_run_coroutine(capsys):
    @run_coroutine
    async def test_this_coroutine(val):
        await asyncio.sleep(10)
        print(val)
        return val

    thread = threading.Thread(target=test_this_coroutine, kwargs={"val": "abcdef"})
    thread.start()
    time.sleep(4)
    signal.pthread_kill(thread.get_ident(), signal.SIGINT)
    # pid = process.pid
    # os.kill(pid, signal.SIGINT)

    captured = capsys.readouterr()
    print(f"{capsys.readouterr()=}")
    assert "Command cancelled" in captured.out
    thread.join()


def test_now_str():
    now = now_str()
    # not much to check here, other than it just being a string
    assert isinstance(now, str)

    now = now_str(posix_friendly=True)
    # Check that the string is in the correct format and don't contain any annoying characters
    assert isinstance(now, str)
    assert ":" not in now
    assert "," not in now
    assert " " not in now
    assert "\n" not in now


def test_expand_path():
    # Pass a relative path, and check that it behaves correctly
    path = expand_path("./", turn_to_abs_path=False)
    assert os.path.samefile(path, os.path.normpath("./"))

    path = expand_path("./", turn_to_abs_path=True)
    assert os.path.samefile(path, os.path.abspath("./"))

    # Pass home, the turn_to_abs_path flag should not matter
    path = expand_path("~/", turn_to_abs_path=False)
    assert os.path.samefile(path, os.path.expanduser("~/"))

    path = expand_path("~/", turn_to_abs_path=True)
    assert os.path.samefile(path, os.path.expanduser("~/"))

    # Pass an absolute path, the turn_to_abs_path flag should not matter
    path = expand_path("/tmp", turn_to_abs_path=False)
    assert os.path.samefile(path, os.path.normpath("/tmp"))

    path = expand_path("/tmp", turn_to_abs_path=False)
    assert os.path.samefile(path, os.path.normpath("/tmp"))

    # Pass a path with a variable in it, the turn_to_abs_path flag should not matter
    path = expand_path("${HOME}", turn_to_abs_path=False)
    assert os.path.samefile(path, os.path.expanduser("~/"))

    path = expand_path("${HOME}", turn_to_abs_path=True)
    assert os.path.samefile(path, os.path.expanduser("~/"))


def test_validate_command_facility():
    # Check that the function raises an exception
    with pytest.raises(click.BadParameter):
        validate_command_facility(None, None, "test test")

    # with pytest.raises(click.BadParameter):
    #     validate_command_facility(None, None, "grpc://mal_formed:123")

    # with pytest.raises(click.BadParameter):
    #     validate_command_facility(None, None, "grpc://malformed:abs")

    with pytest.raises(click.BadParameter):
        validate_command_facility(None, None, "grpc://malformed:1234/123")

    with pytest.raises(click.BadParameter):
        validate_command_facility(None, None, "grpccc://malformed:1234")

    ret = validate_command_facility(None, None, "grpc://good:1234")

    assert ret == "good:1234"


def generate_address(text):
    return "grpc://" + text + ":1234/whatver"


def test_resolve_localhost_to_hostname():
    hostname = socket.gethostname()

    resolved = resolve_localhost_to_hostname(generate_address("localhost"))
    assert resolved == generate_address(hostname)

    resolved = resolve_localhost_to_hostname(generate_address("127.0.0.1"))
    assert resolved == generate_address(hostname)

    resolved = resolve_localhost_to_hostname(generate_address("0.1.90.0"))
    assert resolved == generate_address(hostname)


def test_resolve_localhost_and_127_ip_to_network_ip():
    this_ip = socket.gethostbyname(socket.gethostname())

    resolved = resolve_localhost_and_127_ip_to_network_ip(generate_address("localhost"))
    assert resolved == generate_address(this_ip)

    resolved = resolve_localhost_and_127_ip_to_network_ip(generate_address("127.0.0.1"))
    assert resolved == generate_address(this_ip)

    resolved = resolve_localhost_and_127_ip_to_network_ip(generate_address("0.1.90.0"))
    assert resolved == generate_address(this_ip)


def test_host_is_local():
    this_ip = socket.gethostbyname(socket.gethostname())
    hostname = socket.gethostname()

    assert host_is_local(hostname)
    assert host_is_local("localhost")
    assert host_is_local(this_ip)
    assert host_is_local("0.1.23.4")
    assert host_is_local("127.1.3.6")
    assert not host_is_local("google.com")
    assert not host_is_local("8.8.8.8")


def test_parent_death_pact():
    def child_process():
        parent_death_pact()  # We're testing this one
        child_pid = os.getpid()
        print(f"Child PID: {child_pid}")
        time.sleep(10)

    def parent_process():
        parent_death_pact()  # This isn't the one that we are testing
        # The purpose for this one is if someone ctrl+C the test, then this process should also die
        parent_pid = os.getpid()
        print(f"Parent PID: {parent_pid}")
        child_process_ = multiprocessing.Process(
            target=child_process, name="tester_child_process"
        )
        child_process_.start()
        time.sleep(10)

    process = multiprocessing.Process(
        target=parent_process, name="tester_parent_process"
    )
    process.start()
    time.sleep(0.1)  # Let it run for a while...
    process.kill()
    time.sleep(0.1)  # Let it die for a while...

    # Check that the child process is dead
    assert process.is_alive() == False
    pids = psutil.pids()
    child_pid_still_exists = False
    for pid in pids:
        if psutil.Process(pid).name() == "tester_child_process":
            child_pid_still_exists = True
            break

    assert not child_pid_still_exists


def test_https_or_https_present():
    assert https_or_http_present("http://google.com") == None
    assert https_or_http_present("https://google.com") == None

    with pytest.raises(IncorrectAddress):
        https_or_http_present("ftp://google.com")

    with pytest.raises(IncorrectAddress):
        https_or_http_present("google.com")

    with pytest.raises(IncorrectAddress):
        https_or_http_present("httpss://google.com")


def test_get_control_type_and_uri_from_cli():
    this_address = socket.gethostbyname(socket.gethostname()) + ":1234"

    def generate_cli(control_type, uri):
        return [f"{control_type}://{uri}:1234", "--something-else", "--drunc"]

    control_type, uri = get_control_type_and_uri_from_cli(
        generate_cli("grpc", "localhost")
    )
    assert control_type == ControlType.gRPC
    assert uri == this_address

    control_type, uri = get_control_type_and_uri_from_cli(
        generate_cli("grpc", "0.0.0.0")
    )
    assert control_type == ControlType.gRPC
    assert uri == this_address

    control_type, uri = get_control_type_and_uri_from_cli(
        generate_cli("grpc", "np04-srv-123")
    )
    assert control_type == ControlType.gRPC
    assert uri == "np04-srv-123:1234"

    control_type, uri = get_control_type_and_uri_from_cli(
        generate_cli("rest", "localhost")
    )
    assert control_type == ControlType.REST_API
    assert uri == this_address

    control_type, uri = get_control_type_and_uri_from_cli(
        generate_cli("rest", "0.0.0.0")
    )
    assert control_type == ControlType.REST_API
    assert uri == this_address

    control_type, uri = get_control_type_and_uri_from_cli(
        generate_cli("rest", "np04-srv-123")
    )
    assert control_type == ControlType.REST_API
    assert uri == "np04-srv-123:1234"

    with pytest.raises(DruncSetupException):
        get_control_type_and_uri_from_cli(generate_cli("restt", "bla"))


@pytest.mark.xfail
def test_get_control_type_and_uri_from_connectivity_service():
    raise NotImplementedError()
