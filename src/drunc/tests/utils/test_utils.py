import logging

import pytest


def test_get_random_string():
    from drunc.utils.utils import get_random_string

    string = get_random_string(8)

    # Check that the string is a string
    assert isinstance(string, str)

    # Check that the string is of the correct length
    assert len(string) == 8

    # Check that the string is random
    string2 = get_random_string(8)
    assert string != string2


def test_regex_match():
    from drunc.utils.utils import regex_match

    assert regex_match(".*", "absc")
    assert regex_match(".*", "1234")
    assert regex_match("123.", "1234")


def test_print_traceback(capsys):
    from drunc.utils.utils import print_traceback

    try:
        raise ValueError("Test error")
    except ValueError as e:
        print_traceback(e)
    captured = capsys.readouterr()
    assert "ValueError" in captured.out
    assert "Test error" in captured.out


def test_setup_logger(caplog):
    from drunc.utils.utils import get_logger, setup_root_logger

    drunc_root_logger = setup_root_logger("DEBUG")
    assert drunc_root_logger.getEffectiveLevel() == logging.DEBUG
    assert get_logger("tester0").getEffectiveLevel() == logging.DEBUG

    drunc_root_logger.setLevel("INFO")
    assert drunc_root_logger.getEffectiveLevel() == logging.INFO
    assert get_logger("drunc.tester1").getEffectiveLevel() == logging.INFO

    setup_root_logger.setLevel("WARNING")
    assert drunc_root_logger.getEffectiveLevel() == logging.WARNING
    assert get_logger("tester2").getEffectiveLevel() == logging.WARNING

    setup_root_logger.setLevel("ERROR")
    assert drunc_root_logger.getEffectiveLevel() == logging.ERROR
    assert get_logger("tester3").getEffectiveLevel() == logging.ERROR

    setup_root_logger.setLevel("CRITICAL")
    assert drunc_root_logger.getEffectiveLevel() == logging.CRITICAL
    assert get_logger("tester4").getEffectiveLevel() == logging.CRITICAL

    import tempfile

    with tempfile.TemporaryDirectory() as temp_dir:
        log_path = temp_dir + "/test.log"

        setup_root_logger("CRITICAL", log_path=log_path)
        logger = get_logger("tester5")
        logger.debug("invisible")
        logger.info("invisible")
        logger.warning("invisible")
        logger.error("invisible")
        logger.critical("VISIBLE")

        assert caplog.record_tuples == [
            ("drunc.tester5", logging.CRITICAL, "VISIBLE"),
        ]

        with open(log_path, "r") as f:
            assert "VISIBLE" in f.read()


def test_get_new_port():
    from drunc.utils.utils import get_new_port

    port = get_new_port()

    # Check that the port is free
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        assert s.connect_ex(("localhost", port)) != 0

    # Check that the port is an integer
    assert isinstance(port, int)

    # Check the range of the port
    assert port > 0
    assert port < 65535


def test_run_coroutine():
    from drunc.utils.utils import run_coroutine

    @run_coroutine
    async def test_this_coroutine(val):
        return val

    result = test_this_coroutine("abc")

    assert result == "abc"


@pytest.mark.xfail
def test_interrupt_run_coroutine(capsys):
    # if __name__ == "__main__":
    import asyncio

    from drunc.utils.utils import run_coroutine

    @run_coroutine
    async def test_this_coroutine(val):
        await asyncio.sleep(1)
        print(val)
        return val

    from threading import Thread

    process = Thread(target=test_this_coroutine, kwargs={"val": "abcdef"})
    # process = Process(target=test_this_coroutine, kwargs={"val":'abcdef'})
    process.start()
    # pid = process.pid
    # os.kill(pid, signal.SIGINT)

    captured = capsys.readouterr()
    assert "Command cancelled" in captured.out


def test_now_str():
    from drunc.utils.utils import now_str

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
    import os

    from drunc.utils.utils import expand_path

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
    # if True:
    from click import BadParameter

    from drunc.utils.utils import validate_command_facility

    # Check that the function raises an exception
    with pytest.raises(BadParameter):
        validate_command_facility(None, None, "test test")

    # with pytest.raises(BadParameter):
    #     validate_command_facility(None, None, "grpc://mal_formed:123")

    # with pytest.raises(BadParameter):
    #     validate_command_facility(None, None, "grpc://malformed:abs")

    with pytest.raises(BadParameter):
        validate_command_facility(None, None, "grpc://malformed:1234/123")

    with pytest.raises(BadParameter):
        validate_command_facility(None, None, "grpccc://malformed:1234")

    ret = validate_command_facility(None, None, "grpc://good:1234")

    assert ret == "good:1234"


def generate_address(text):
    return "grpc://" + text + ":1234/whatver"


def test_resolve_localhost_to_hostname():
    from socket import gethostname

    from drunc.utils.utils import resolve_localhost_to_hostname

    hostname = gethostname()

    resolved = resolve_localhost_to_hostname(generate_address("localhost"))
    assert resolved == generate_address(hostname)

    resolved = resolve_localhost_to_hostname(generate_address("127.0.0.1"))
    assert resolved == generate_address(hostname)

    resolved = resolve_localhost_to_hostname(generate_address("0.1.90.0"))
    assert resolved == generate_address(hostname)


def test_resolve_localhost_and_127_ip_to_network_ip():
    from socket import gethostbyname, gethostname

    from drunc.utils.utils import resolve_localhost_and_127_ip_to_network_ip

    this_ip = gethostbyname(gethostname())

    resolved = resolve_localhost_and_127_ip_to_network_ip(generate_address("localhost"))
    assert resolved == generate_address(this_ip)

    resolved = resolve_localhost_and_127_ip_to_network_ip(generate_address("127.0.0.1"))
    assert resolved == generate_address(this_ip)

    resolved = resolve_localhost_and_127_ip_to_network_ip(generate_address("0.1.90.0"))
    assert resolved == generate_address(this_ip)


def test_host_is_local():
    from socket import gethostbyname, gethostname

    from drunc.utils.utils import host_is_local

    this_ip = gethostbyname(gethostname())
    hostname = gethostname()

    assert host_is_local(hostname)
    assert host_is_local("localhost")
    assert host_is_local(this_ip)
    assert host_is_local("0.1.23.4")
    assert host_is_local("127.1.3.6")
    assert not host_is_local("google.com")
    assert not host_is_local("8.8.8.8")


def test_parent_death_pact():
    from multiprocessing import Process
    from os import getpid
    from time import sleep

    from drunc.utils.utils import parent_death_pact

    def child_process():
        parent_death_pact()  # We're testing this one
        child_pid = getpid()
        print(f"Child PID: {child_pid}")
        sleep(10)

    def parent_process():
        parent_death_pact()  # This isn't the one that we are testing
        # The purpose for this one is if someone ctrl+C the test, then this process should also die
        parent_pid = getpid()
        print(f"Parent PID: {parent_pid}")
        child_process_ = Process(target=child_process, name="tester_child_process")
        child_process_.start()
        sleep(10)

    process = Process(target=parent_process, name="tester_parent_process")
    process.start()
    sleep(0.1)  # Let it run for a while...
    process.kill()
    sleep(0.1)  # Let it die for a while...

    # Check that the child process is dead
    assert process.is_alive() == False
    import psutil

    pids = psutil.pids()
    child_pid_still_exists = False
    for pid in pids:
        if psutil.Process(pid).name() == "tester_child_process":
            child_pid_still_exists = True
            break

    assert not child_pid_still_exists


def test_https_or_https_present():
    from drunc.utils.utils import IncorrectAddress, https_or_http_present

    assert https_or_http_present("http://google.com") == None
    assert https_or_http_present("https://google.com") == None

    with pytest.raises(IncorrectAddress):
        https_or_http_present("ftp://google.com")

    with pytest.raises(IncorrectAddress):
        https_or_http_present("google.com")

    with pytest.raises(IncorrectAddress):
        https_or_http_present("httpss://google.com")


@pytest.mark.xfail
def test_http_post():
    raise NotImplementedError()


@pytest.mark.xfail
def test_http_get():
    raise NotImplementedError()


@pytest.mark.xfail
def test_http_patch():
    raise NotImplementedError()


@pytest.mark.xfail
def test_http_delete():
    raise NotImplementedError()


def test_get_control_type_and_uri_from_cli():
    from socket import gethostbyname, gethostname

    from drunc.exceptions import DruncSetupException
    from drunc.utils.utils import ControlType, get_control_type_and_uri_from_cli

    this_address = gethostbyname(gethostname()) + ":1234"

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
