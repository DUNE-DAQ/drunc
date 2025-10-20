import getpass
from os import environ

import conffwk
from sh import Command, ErrorReturnCode

from drunc.exceptions import DruncSetupException
from drunc.fsm.core import FSMAction
from drunc.fsm.exceptions import ThreadPinningFailed
from drunc.process_manager.oks_parser import collect_apps
from drunc.process_manager.utils import get_rte_script
from drunc.utils.utils import get_logger

# Optional K8s client imports (prefer API over kubectl binary when available)
try:
    from kubernetes import client as k8s_client
    from kubernetes import config as k8s_config
    from kubernetes.stream import stream as k8s_stream
except Exception:
    k8s_client = None
    k8s_config = None
    k8s_stream = None


class ThreadPinning(FSMAction):
    def __init__(self, configuration):
        super().__init__(name="thread-pinning")
        self.log = get_logger("controller.thread-pinning")
        self.conf_dict = {p.name: p.value for p in configuration.parameters}

    def pin_thread(
        self,
        thread_pinning_file,
        configuration,
        session_uid,
        session_name=None,
        is_k8s=False,
    ):
        db = conffwk.Configuration(configuration)
        # Always use the OKS UID for DB lookups
        session_dal = db.get_dal(class_name="Session", uid=session_uid)

        apps = collect_apps(
            config_filename=configuration,
            session_name=session_uid,
            db=db,
            session_obj=session_dal,
            segment_obj=session_dal.segment,
            env=environ,
            tree_prefix=[],
        )

        if session_dal.rte_script:
            rte = session_dal.rte_script

        else:
            rte_script = get_rte_script()
            if not rte_script:
                raise DruncSetupException("No RTE script found.")

            rte = rte_script

        cmd = f"source {rte}; " if rte else ""
        cmd += f"readout-affinity.py --pinfile {thread_pinning_file}"

        try:
            user = environ.get("USER")
        except KeyError:
            user = getpass.getuser()
        self.log.info(f"USER is set to {user}")

        hosts = set()
        for app in apps:
            hosts.add(app["host"])

        failed_hosts = set()

        if is_k8s:
            namespace = session_name
            podnames = []
            for app in apps:
                if "name" in app:
                    podnames.append(app["name"])

            succeeded_pods = set()

            # 1) Try Kubernetes Python API first (if available)
            if (
                k8s_client is not None
                and k8s_config is not None
                and k8s_stream is not None
            ):
                try:
                    try:
                        k8s_config.load_incluster_config()
                        self.log.info("Loaded in-cluster Kubernetes config")
                    except Exception:
                        k8s_config.load_kube_config()
                        self.log.info("Loaded local kubeconfig")

                    core_v1 = k8s_client.CoreV1Api()
                    for pod in podnames:
                        try:
                            self.log.info(
                                f"Executing '{cmd}' in pod {namespace}.{pod} via K8s API"
                            )
                            proc = k8s_stream(
                                core_v1.connect_get_namespaced_pod_exec,
                                pod,
                                namespace,
                                command=["/bin/sh", "-lc", f"{{ {cmd} ; }}"],
                                stderr=True,
                                stdin=False,
                                stdout=True,
                                tty=False,
                            )
                            self.log.info(proc)
                            succeeded_pods.add(pod)
                        except Exception as e:
                            self.log.info(
                                f"K8s API exec failed for {namespace}.{pod}, will try alternatives: {e}"
                            )
                except Exception as e:
                    self.log.info(f"K8s API not usable, will try alternatives: {e}")

            # Determine which pods still need handling
            remaining_pods = [p for p in podnames if p not in succeeded_pods]

            # 2) Try kubectl for remaining pods
            if remaining_pods:
                my_kubectl = None
                try:
                    my_kubectl = Command("kubectl")
                except Exception:
                    try:
                        my_kubectl = Command("/usr/bin/kubectl")
                    except Exception:
                        my_kubectl = None

                if my_kubectl is not None:
                    for pod in remaining_pods:
                        try:
                            self.log.info(
                                f"Executing '{cmd}' in pod {namespace}.{pod} via kubectl"
                            )
                            proc = my_kubectl(
                                "exec",
                                "-n",
                                namespace,
                                pod,
                                "--",
                                "/bin/sh",
                                "-lc",
                                f"{{ {cmd} ; }}",
                                _err_to_out=True,
                            )
                            self.log.info(proc)
                            succeeded_pods.add(pod)
                        except ErrorReturnCode as e:
                            self.log.error(e.stdout.decode("ascii"))
                            self.log.error(e.stderr.decode("ascii"))
                            failed_hosts.add(
                                f"{namespace}.{pod}: {e.stderr.decode('ascii')}"
                            )
                            continue
                        except Exception as e:
                            self.log.critical(str(e))
                            failed_hosts.add(f"{namespace}.{pod}: {e}")
                            continue
                else:
                    for pod in remaining_pods:
                        failed_hosts.add(
                            f"{namespace}.{pod}: kubectl not found and K8s API exec was unavailable"
                        )

        else:
            my_ssh = Command("/usr/bin/ssh")

            for host in hosts:
                arguments = [
                    user + "@" + host,
                    "-tt",
                    "-o StrictHostKeyChecking=no",
                    f"{{ {cmd} ; }}",
                ]
                try:
                    self.log.info(f"Executing '{cmd}'")
                    self.log.info(
                        f"Applying thread pinning {cmd} file {thread_pinning_file} on {host}"
                    )
                    proc = my_ssh(
                        *arguments,
                        _err_to_out=True,
                    )
                    self.log.info(proc)
                except ErrorReturnCode as e:
                    self.log.error(e.stdout.decode("ascii"))
                    self.log.error(e.stderr.decode("ascii"))
                    failed_hosts.add(f"{host}: {e.stderr.decode('ascii')}")
                    continue
                except Exception as e:
                    self.log.critical(str(e))
                    failed_hosts.add(f"{host}: {e}")
                    continue
                self.log.debug(proc)

        failed_hosts_error_str = ", ".join(failed_hosts)
        if failed_hosts:
            raise ThreadPinningFailed(failed_hosts_error_str)

    def post_conf(self, _input_data, _context, **kwargs):
        if "post_conf" in self.conf_dict:
            self.pin_thread(
                self.conf_dict["post_conf"],
                _context.configuration.initial_data,
                session_uid=_context.configuration.oks_key.session,
                session_name=getattr(_context.configuration, "session_name", None),
                is_k8s=True,
            )
        return _input_data

    def post_start(self, _input_data, _context, **kwargs):
        if "post_start" in self.conf_dict:
            self.pin_thread(
                self.conf_dict["post_start"],
                _context.configuration.initial_data,
                session_uid=_context.configuration.oks_key.session,
                session_name=getattr(_context.configuration, "session_name", None),
            )
        return _input_data

    def pre_conf(self, _input_data, _context, **kwargs):
        if "pre_conf" in self.conf_dict:
            self.pin_thread(
                self.conf_dict["pre_conf"],
                _context.configuration.initial_data,
                session_uid=_context.configuration.oks_key.session,
                session_name=getattr(_context.configuration, "session_name", None),
                is_k8s=True,
            )
        return _input_data
