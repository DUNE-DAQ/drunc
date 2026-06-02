import copy as cp
import os
import re
from collections.abc import Callable
from functools import update_wrapper
from typing import ParamSpec, cast

import click
from druncschema.process_manager_pb2 import (
    ProcessInstance,
    ProcessInstanceList,
    ProcessQuery,
    ProcessUUID,
)
from rich.table import Table

from drunc.exceptions import DruncCommandException, DruncException, DruncSetupException
from drunc.process_manager.configuration import (
    ProcessManagerConfHandler,
    ProcessManagerTypes,
    get_process_manager_configuration,
)
from drunc.utils.configuration import parse_conf_url
from drunc.utils.utils import now_str

P = ParamSpec("P")


def generate_process_query(
    f: Callable[P, object],
    at_least_one: bool,
    all_processes_by_default: bool = False,
) -> Callable[P, object]:
    @click.pass_context
    def new_func(
        ctx: click.Context,
        session: str | None,
        name: tuple[str, ...],
        user: str | None,
        uuid: tuple[str, ...],
        **kwargs: object,
    ) -> object:
        is_trivial_query = bool(
            (len(uuid) == 0)
            and (session is None)
            and (len(name) == 0)
            and (user is None)
        )

        if is_trivial_query and at_least_one:
            raise click.BadParameter(
                "You need to provide at least a '--uuid', '--session', '--user' or '--name'!\nAll these values are presented with 'ps'.\nIf you want to kill everything, use 'ps' and 'kill'."
            )

        query_names = list(name)
        if all_processes_by_default and is_trivial_query:
            query_names = [".*"]

        uuids = [ProcessUUID(uuid=uuid_) for uuid_ in uuid]
        crash = kwargs.pop("crash", False)
        crash_flag = crash if isinstance(crash, bool) else False

        query = ProcessQuery(
            session=session or "",
            names=query_names,
            user=user or "",
            uuids=uuids,
            crash=crash_flag,
        )
        # print(query)
        return ctx.invoke(f, query=query, **kwargs)

    return cast(Callable[P, object], update_wrapper(new_func, f))


def make_tree(values: list[ProcessInstance]) -> list[str]:
    lines = []
    for result in values:
        m = result.process_description.metadata
        tree_levels = m.tree_id.split(".")
        indent_level = len(tree_levels) - 1
        indentation = "  " * indent_level
        lines.append(indentation + m.name)
    return lines


def order_process_by_name(processes: list[ProcessInstance]) -> list[ProcessInstance]:
    """Given a list of processes, perform a tiered order by the name"""
    by_session: dict[str, list[ProcessInstance]] = {}
    for process in processes:
        m = process.process_description.metadata
        by_session.setdefault(m.session, []).append(process)

    ordered: list[ProcessInstance] = []
    for session in sorted(by_session.keys()):
        session_processes = by_session[session]
        node_by_id: dict[str, list[ProcessInstance]] = {}
        children: dict[str, list[str]] = {}
        roots: list[str] = []

        for process in session_processes:
            tree_id = process.process_description.metadata.tree_id or ""
            node_by_id.setdefault(tree_id, []).append(process)

        for tree_id, processes in node_by_id.items():
            node_by_id[tree_id] = sorted(
                processes,
                key=lambda p: (
                    p.process_description.metadata.name,
                    p.uuid.uuid,
                ),
            )

        for tree_id in node_by_id.keys():
            parent_id = tree_id.rsplit(".", 1)[0] if "." in tree_id else None
            if not parent_id or parent_id not in node_by_id:
                roots.append(tree_id)
            else:
                children.setdefault(parent_id, []).append(tree_id)

        def sort_key(tree_id: str) -> tuple[str, str]:
            m = node_by_id[tree_id][0].process_description.metadata
            return (m.name, tree_id)

        def walk(tree_id: str) -> None:
            ordered.extend(node_by_id[tree_id])
            for child_id in sorted(children.get(tree_id, []), key=sort_key):
                walk(child_id)

        for root_id in sorted(roots, key=sort_key):
            walk(root_id)

    return ordered


def tabulate_process_instance_list(
    pil: ProcessInstanceList, title: str, long: bool = False, width: int | None = None
) -> Table:
    t = Table(title=title, width=width)
    t.add_column("session")
    t.add_column("friendly name")
    t.add_column("user")
    t.add_column("host")
    t.add_column("uuid")
    t.add_column("alive")
    t.add_column("exit-code")

    sorted_pil = order_process_by_name(list(pil.values))

    show_remote_pid = long and any(
        process.HasField("remote_pid") for process in sorted_pil
    )
    if show_remote_pid:
        t.add_column("remote-pid")
    if long:
        t.add_column("executable")

    tree_str = make_tree(sorted_pil)
    try:
        for process, line in zip(sorted_pil, tree_str):
            m = process.process_description.metadata
            alive = (
                "True"
                if process.status_code == ProcessInstance.StatusCode.RUNNING
                else "[danger]False[/danger]"
            )
            row = [m.session, line, m.user, m.hostname, process.uuid.uuid]

            process_return_code = (
                process.return_code if process.HasField("return_code") else "NONE"
            )
            row += [alive, f"{process_return_code}"]
            if show_remote_pid:
                row += [
                    process.remote_pid
                    if process.HasField("remote_pid")
                    else "Not available"
                ]
            if long:
                executables = [
                    e.exec for e in process.process_description.executable_and_arguments
                ]
                row += ["; ".join(executables)]
            t.add_row(*row)
    except TypeError:
        raise DruncCommandException(
            "Unable to extract the parameters for tabulate_process_instance_list, exiting."
        )
    return t


def strip_env_for_rte(env: dict[str, str]) -> dict[str, str]:
    env_stripped = cp.deepcopy(env)
    for key in env.keys():
        if key in [
            "PATH",
            "CET_PLUGIN_PATH",
            "DUNEDAQ_SHARE_PATH",
            "LD_LIBRARY_PATH",
            "LIBRARY_PATH",
            "PYTHONPATH",
        ]:
            del env_stripped[key]
        if re.search(".*_SHARE", key) and key in env_stripped:
            del env_stripped[key]
    return env_stripped


def get_version() -> str:
    version = os.getenv("DUNE_DAQ_BASE_RELEASE")
    if not version:
        raise RuntimeError(
            "Utils: dunedaq version not in the variable env DUNE_DAQ_BASE_RELEASE! Exit drunc and\nexport DUNE_DAQ_BASE_RELEASE=dunedaq-vX.XX.XX\n"
        )
    return version


def get_releases_dir() -> str:
    releases_dir = os.getenv("SPACK_RELEASES_DIR")
    if not releases_dir:
        raise RuntimeError(
            "Utils: cannot get env SPACK_RELEASES_DIR! Exit drunc and\nrun dbt-workarea-env or dbt-setup-release."
        )
    return releases_dir


def release_or_dev() -> str:
    is_release = os.getenv("DBT_SETUP_RELEASE_SCRIPT_SOURCED")
    if is_release:
        return "rel"
    is_devenv = os.getenv("DBT_WORKAREA_ENV_SCRIPT_SOURCED")
    if is_devenv:
        return "dev"
    return "rel"


def get_rte_script() -> str:
    script = ""
    if release_or_dev() == "rel":
        ver = get_version()
        releases_dir = get_releases_dir()
        script = os.path.join(releases_dir, ver, "daq_app_rte.sh")

    else:
        dbt_install_dir = os.getenv("DBT_INSTALL_DIR")
        if not dbt_install_dir:
            raise DruncSetupException("DBT_INSTALL_DIR is not set in the environment")
        script = os.path.join(dbt_install_dir, "daq_app_rte.sh")

    if not os.path.exists(script):
        raise DruncSetupException(f"Tentative RTE script: {script}")
    return script


def get_log_path(
    user: str,
    session_name: str,
    application_name: str,
    override_logs: bool,
    app_log_path: str | None = None,
    session_log_path: str | None = None,
) -> str:
    pwd = os.getcwd()
    if app_log_path == "./":
        app_log_path = pwd
    log_path = None
    if app_log_path:  # if the user wants to write to a specific path, we never override
        log_path = f"{app_log_path}/log_{user}_{session_name}_{application_name}_{now_str(True)}.txt"
    elif (
        session_log_path
    ):  # if the user wants the session to write to a specific path, we never override
        log_path = f"{session_log_path}/log_{user}_{session_name}_{application_name}_{now_str(True)}.txt"
    elif override_logs:  # else we check for the override flag
        log_path = f"{pwd}/log_{user}_{session_name}_{application_name}.txt"
    else:
        log_path = (
            f"{pwd}/log_{user}_{session_name}_{application_name}_{now_str(True)}.txt"
        )
    return log_path


# # ------------------------------------------------
# # pexpect.spawn(...,preexec_fn=on_parent_exit('SIGTERM'))

# Constant taken from http://linux.die.net/include/linux/prctl.h
PR_SET_PDEATHSIG = 1


class PrCtlError(DruncException):
    pass


def on_parent_exit(signum: int) -> Callable[[], None]:
    """Return a function to be run in a child process which will trigger
    SIGNAME to be sent when the parent process dies
    """

    def set_parent_exit_signal() -> None:
        from ctypes import cdll

        # http://linux.die.net/man/2/prctl
        result = cdll["libc.so.6"].prctl(PR_SET_PDEATHSIG, signum)
        if result != 0:
            raise PrCtlError("prctl failed with error code %s" % result)

    return set_parent_exit_signal


# ------------------------------------------------


def validate_k8s_session_name(session: str) -> bool:
    """
    Validate that the session/namespace name is valid according to RFC1123 label standard.

    Args:
        session (str): The session/namespace name to validate.

    Returns:
        bool: True if the session name is valid, False otherwise.
    """
    session_re = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
    if not session_re.match(session):
        return False
    return True


def get_pm_type_from_name(pm_name: str) -> ProcessManagerTypes:
    """
    Get the ProcessManagerTypes enum value from a string name.

    Args:
        pm_name (str): The name of the process manager type (e.g., "SSH", "K8s").

    Returns:
        ProcessManagerTypes: The corresponding enum value.
    """
    pm_conf_file = get_process_manager_configuration(pm_name)

    conf_path, conf_type = parse_conf_url(pm_conf_file)
    pmch = ProcessManagerConfHandler(
        log_path="./", type=conf_type, data=conf_path.split(":")[1]
    )

    return cast(ProcessManagerTypes, pmch.data.type)


def format_hostname(hostname: str) -> str:
    """
    Format the host name to truly reflect what the host name is, removing any extensions
    that do not reflect the true host alias.

    Args:
        hostname (str): The hostname to format.

    Returns:
        str: The formatted hostname.

    Raises:
        DruncCommandException: If the hostname is empty or None.

    Example:
        If the input hostname is "np02-srv-005-1", the output will be "np02-srv-005".
    """
    # Validate that the hostname is not empty or None
    if not hostname:
        raise DruncCommandException("Hostname cannot be empty or None.")

    # Make a copy of the hostname to modify
    formatted_hostname = hostname

    # Strip common suffixes that do not reflect the true host alias
    if hostname.endswith("-1"):
        formatted_hostname = hostname[:-2]

    return formatted_hostname
