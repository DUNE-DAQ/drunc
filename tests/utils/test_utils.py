import logging
import multiprocessing
import os
import socket
import time

import click
import psutil
import pytest
from rich.console import Console, Group
from rich.measure import Measurement
from rich.table import Table

from drunc.exceptions import DruncSetupException
from drunc.utils.utils import (
    ControlType,
    IncorrectAddress,
    expand_path,
    format_table_width,
    get_control_type_and_uri_from_cli,
    get_new_port,
    get_random_string,
    host_is_local,
    https_or_http_present,
    now_str,
    parent_death_pact,
    regex_match,
    resolve_context_peer,
    resolve_localhost_and_127_ip_to_network_ip,
    resolve_localhost_to_hostname,
    validate_command_facility,
    wait,
)


@pytest.mark.parametrize("fit", [True, False])
def test_format_table_width(fit):
    console = Console(width=20)
    table = Table(expand=True)
    table.add_column("Process")
    table.add_row("a-process-name-longer-than-the-console")

    assert format_table_width(console, table, fit) is table
    assert all(column.no_wrap for column in table.columns)
    expected_width = (
        console.width
        if fit
        else Measurement.get(
            console, console.options.update(max_width=10000), table
        ).maximum
    )
    assert table.width == expected_width
    if not fit:
        assert table.width > console.width


def test_format_table_width_nested_group():
    console = Console(width=30)
    first = Table("First", expand=True)
    second = Table("Second", expand=True)
    group = Group(first, Group("unchanged text", second))

    assert format_table_width(console, group, fit=True) is group
    for table in (first, second):
        assert table.width == console.width
        assert all(column.no_wrap for column in table.columns)


@pytest.mark.parametrize("progress_steps", [None, 0, -1, 3])
def test_wait(monkeypatch, caplog, progress_steps):
    sleep_calls = []
    monkeypatch.setattr("drunc.utils.utils.time.sleep", sleep_calls.append)
    logger = logging.getLogger("test_wait")

    with caplog.at_level(logging.INFO, logger=logger.name):
        wait(logger, 5, progress_steps)

    assert sum(sleep_calls) == pytest.approx(5)
    progress = [
        record.message for record in caplog.records if "Progress:" in record.message
    ]
    if progress_steps == 3:
        assert len(sleep_calls) == 3
        assert len(progress) == 3
        assert progress[-1] == "Progress: 5 / 5 seconds"
    else:
        assert sleep_calls == [5]
        assert not progress
    assert (
        caplog.records[-1].message == "Command [green]wait[/green] ran for 5 seconds."
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


def test_resolve_context_peer():
    assert resolve_context_peer("grpc:np04-srv-028:50000") == "np04-srv-028:50000"
    assert resolve_context_peer("np04-srv-028:50000") == "np04-srv-028:50000"
    assert resolve_context_peer("") == ""


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
        try:
            if psutil.Process(pid).name() == "tester_child_process":
                child_pid_still_exists = True
                break
        except psutil.NoSuchProcess:
            continue

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
