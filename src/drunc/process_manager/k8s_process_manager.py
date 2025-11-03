# Standard Library Imports
import getpass
import os
import re
import socket
import threading
import urllib.error
import urllib.request
import uuid
from time import sleep, time
from typing import Dict, List, Optional, Tuple

# Local Application Imports
from druncschema.broadcast_pb2 import BroadcastType
from druncschema.process_manager_pb2 import (
    BootRequest,
    LogLines,
    LogRequest,
    ProcessDescription,
    ProcessInstance,
    ProcessInstanceList,
    ProcessQuery,
    ProcessRestriction,
    ProcessUUID,
)

# Third-Party Imports
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

from drunc.k8s_exceptions import (
    DruncK8sException,
    DruncK8sNamespaceException,
    DruncK8sNodeException,
    DruncK8sPodException,
)
from drunc.process_manager.process_manager import ProcessManager
from drunc.process_manager.utils import validate_k8s_session_name
from drunc.utils.utils import get_logger, resolve_localhost_to_hostname


class K8sPodWatcherThread(threading.Thread):
    def __init__(self, pm) -> None:
        threading.Thread.__init__(self)
        self.pm = pm
        self.daemon = True
        self.processed_uuids = set()

    def run(self) -> None:
        self.pm.log.info("K8sPodWatcherThread started")
        while True:
            try:
                w = watch.Watch()
                stream = w.stream(
                    self.pm._core_v1_api.list_pod_for_all_namespaces,
                    label_selector=self.pm._get_creator_label_selector(),
                )
                for event in stream:
                    pod = event["object"]
                    metadata = pod.metadata
                    status = pod.status
                    phase = status.phase
                    proc_uuid = metadata.labels.get(f"uuid.{self.pm.drunc_label}")
                    session = metadata.namespace

                    if not proc_uuid:
                        continue

                    if proc_uuid in self.processed_uuids:
                        continue

                    self.pm.log.debug(
                        f"Watcher saw event: type={event['type']}, phase={phase}, uuid={proc_uuid}"
                    )

                    is_terminal_phase = phase in ["Succeeded", "Failed"]
                    is_deleted_event = event["type"] == "DELETED"

                    if is_terminal_phase or is_deleted_event:
                        exit_code = -1
                        reason = "Unknown"
                        if (
                            status.container_statuses
                            and status.container_statuses[0].state.terminated
                        ):
                            terminated_state = status.container_statuses[
                                0
                            ].state.terminated
                            exit_code = terminated_state.exit_code
                            reason = terminated_state.reason
                        elif is_deleted_event:
                            reason = "PodDeleted"

                        self.processed_uuids.add(proc_uuid)
                        self.pm.notify_termination(
                            proc_uuid, exit_code, reason, session
                        )

            except ApiException as e:
                if e.status == 410:
                    pass
                else:
                    self.pm.log.error(
                        f"K8s API error in watcher: {e}. Restarting watch."
                    )
                sleep(3)

            except Exception as e:
                self.pm.log.error(f"K8s watcher thread error: {e}. Restarting watch.")
                sleep(self.pm.watcher_retry_sleep)


class K8sProcessManager(ProcessManager):
    def __init__(self, configuration, **kwargs) -> None:
        """
        Manages processes as Kubernetes Pods.
        This ProcessManager interfaces with the Kubernetes API to start, stop, and monitor
        applications running in Pods. It includes special handling for a local connectivity
        service, which involves:
        1.  Using a NodePort service for the orchestrator for external access.
        """
        self.session = getpass.getuser()
        super().__init__(configuration=configuration, session=self.session, **kwargs)
        self.log = get_logger("process_manager.k8s-process-manager")

        config.load_kube_config()

        self._k8s_client = client
        self._core_v1_api = client.CoreV1Api()
        self._meta_v1_api = client.V1ObjectMeta
        self._pod_spec_v1_api = client.V1PodSpec
        self._api_error_v1_api = client.rest.ApiException

        self.managed_sessions = set()
        self.watchers = []
        self._start_watcher()
        self.sessions_pending_deletion = set()
        self.uuids_pending_deletion = set()
        self.termination_complete_event = threading.Event()
        self.final_exit_codes = {}
        self.local_connection_server_is_booted = False

        # Host verification cache: {hostname: (is_valid, timestamp)}
        self._host_cache = {}
        self._host_cache_lock = threading.Lock()

        # Get settings from configuration
        settings = getattr(self.configuration.data, "settings", {})

        # Labels
        labels = settings.get("labels", {})
        self.drunc_label = labels.get("drunc_label", "drunc.daq")

        # Connection server
        connection_server = settings.get("connection_server", {})
        self.connection_server_name = connection_server.get(
            "name", "local-connection-server"
        )
        self.connection_server_port = None
        self.connection_server_node_port = None

        # Pod management
        pod_management = settings.get("pod_management", {})
        self.kill_timeout = pod_management.get("kill_timeout", 20)
        self.pod_ready_timeout = pod_management.get("pod_ready_timeout", 60)

        # Cleanup
        cleanup = settings.get("cleanup", {})
        self.restart_cleanup_time = cleanup.get("restart_cleanup_time", 10.0)
        self.restart_cleanup_polling = cleanup.get("restart_cleanup_polling", 0.5)

        # Checking
        checking = settings.get("checking", {})
        self.watcher_retry_sleep = checking.get("watcher_retry_sleep", 5)
        self.pod_status_check_sleep = checking.get("pod_status_check_sleep", 1)
        self._host_cache_expiry = checking.get("host_cache_expiry", 300)

        self.log.debug(f"Using kill_timeout of {self.kill_timeout} seconds.")

        namespaces = self._core_v1_api.list_namespace(
            label_selector=f"creator.{self.drunc_label}={self.__class__.__name__}"
        )
        namespace_names = [ns.metadata.name for ns in namespaces.items]
        namespace_list_str = "\n - ".join(namespace_names)

        if namespace_list_str:
            self.log.info(
                f"Active namespaces created by drunc:\n - {namespace_list_str}"
            )
        else:
            self.log.info("No active namespace created by drunc")

    def _start_watcher(self) -> None:
        """Starts the background thread that watches for Pod status changes."""
        self.log.debug("Starting K8s pod watcher thread")
        t = K8sPodWatcherThread(pm=self)
        t.start()
        self.watchers.append(t)

    def notify_termination(self, proc_uuid, exit_code, reason, session) -> None:
        """Callback for when a pod terminates."""
        self.log.debug(
            f"notify_termination called for '{proc_uuid}'. Pending={self.uuids_pending_deletion}"
        )

        if proc_uuid in self.boot_request:
            self.final_exit_codes[proc_uuid] = exit_code

            meta = self.boot_request[proc_uuid].process_description.metadata
            end_str = f"Pod '{meta.name}' (session: '{session}', user: '{meta.user}', uuid: {proc_uuid}) terminated with exit code {exit_code}. Reason: {reason}"
            self.log.info(end_str)
            self.broadcast(end_str, BroadcastType.SUBPROCESS_STATUS_UPDATE)

        if proc_uuid in self.uuids_pending_deletion:
            self.uuids_pending_deletion.remove(proc_uuid)
            self.log.debug(
                f"Watcher confirmed termination of {proc_uuid}. {len(self.uuids_pending_deletion)} pods remaining."
            )
            if not self.uuids_pending_deletion:
                self.log.debug("All pending pods terminated, setting event.")
                self.termination_complete_event.set()

    def is_alive(self, podname, session) -> bool:
        """Checks if a pod is currently in the 'Running' phase."""
        try:
            pod_status = self._core_v1_api.read_namespaced_pod_status(podname, session)
            return pod_status.status.phase == "Running"
        except self._api_error_v1_api as e:
            if e.status == 404:
                return False
            self.log.error(f"Error checking status for pod {session}.{podname}: {e}")
            return False

    def _add_label(self, obj_name, obj_type, key, label, session=None) -> None:
        """Adds a label to a Kubernetes object (Pod or Namespace)."""
        body = {"metadata": {"labels": {f"{key}.{self.drunc_label}": label}}}

        if obj_type == "pod":
            if not session:
                raise DruncK8sNamespaceException(
                    "Session (namespace) must be provided to label a pod."
                )

            try:
                self._core_v1_api.patch_namespaced_pod(
                    name=obj_name, namespace=session, body=body
                )
                self.log.info(
                    f'Added label "{key}.{self.drunc_label}:{label}" to pod "{session}.{obj_name}"'
                )
            except self._api_error_v1_api as e:
                self.log.error(
                    f"Failed to apply label to pod {session}/{obj_name}: {e}"
                )
        elif obj_type == "namespace":
            try:
                self._core_v1_api.patch_namespace(name=obj_name, body=body)
                self.log.info(
                    f'Added label "{key}.{self.drunc_label}:{label}" to namespace "{obj_name}"'
                )
            except self._api_error_v1_api as e:
                self.log.error(f"Failed to apply label to namespace {obj_name}: {e}")
        else:
            raise DruncK8sException(f"Cannot add label to object type: {obj_type}")

    def _add_creator_label(self, obj_name, obj_type) -> None:
        """Adds a 'creator' label to a Kubernetes object."""
        self._add_label(obj_name, obj_type, "creator", self.__class__.__name__)

    def _get_creator_label_selector(self) -> str:
        """Returns the label selector for objects created by this class."""
        return f"creator.{self.drunc_label}={self.__class__.__name__}"

    def _is_host_cached(self, host):
        """Check if host is cached and not expired."""
        with self._host_cache_lock:
            if host not in self._host_cache:
                return None
            is_valid, timestamp = self._host_cache[host]
            if time() - timestamp > self._host_cache_expiry:
                del self._host_cache[host]
                return None
            return is_valid

    def _verify_host_in_cluster(self, target_host):
        """Verifies that the target host is available in the Kubernetes cluster."""
        cached = self._is_host_cached(target_host)
        if cached is not None:
            if cached:
                self.log.debug(f"Host '{target_host}' cached (valid)")
                return True
            else:
                raise DruncK8sNodeException(
                    f"Host '{target_host}' was previously verified as unavailable"
                )

        try:
            target_node = self._core_v1_api.read_node(name=target_host)
            # Check node is ready and schedulable
            is_ready = any(
                c.type == "Ready" and c.status == "True"
                for c in target_node.status.conditions or []
            )
            is_schedulable = not (target_node.spec and target_node.spec.unschedulable)

            if not is_ready or not is_schedulable:
                with self._host_cache_lock:
                    self._host_cache[target_host] = (False, time())
                reason = "not ready" if not is_ready else "cordoned"
                raise DruncK8sNodeException(f"Host '{target_host}' {reason}")

            with self._host_cache_lock:
                self._host_cache[target_host] = (True, time())
            self.log.info(f"Host '{target_host}' verified and available")
            return True

        except self._api_error_v1_api as e:
            if e.status == 404:
                with self._host_cache_lock:
                    self._host_cache[target_host] = (False, time())
                raise DruncK8sNodeException(
                    f"Target host '{target_host}' is not part of the Kubernetes cluster"
                )
            elif e.status in [401, 403]:
                raise DruncK8sException(
                    f"Permission denied accessing cluster to verify '{target_host}': {e}"
                )
            raise DruncK8sException(f"Failed to verify host '{target_host}': {e}")
        except Exception as e:
            raise DruncK8sException(f"Error verifying host '{target_host}': {e}")

    def _create_namespace(self, session) -> None:
        """Creates a Kubernetes namespace if it doesn't already exist."""
        if session in self.sessions_pending_deletion:
            self.sessions_pending_deletion.remove(session)

        if session in self.managed_sessions:
            return

        try:
            self._core_v1_api.read_namespace(name=session)
            raise DruncK8sNamespaceException(
                f"Namespace '{session}' already exists. Please use a different session name."
            )

        except self._api_error_v1_api as e:
            if e.status == 404:
                self.log.info(f'Creating "{session}" namespace.')
                namespace_manifest = client.V1Namespace(
                    api_version="v1",
                    kind="Namespace",
                    metadata=self._meta_v1_api(
                        name=session,
                        labels={"pod-security.kubernetes.io/enforce": "privileged"},
                    ),
                )
                self._core_v1_api.create_namespace(body=namespace_manifest)
                self._add_creator_label(session, "namespace")
                self.managed_sessions.add(session)
            else:
                raise DruncK8sException(f"Failed to check namespace '{session}': {e}")

    def _create_headless_service(self, podname, session, pod_uid) -> None:
        """Creates a headless service for a pod."""
        service_manifest = client.V1Service(
            api_version="v1",
            kind="Service",
            metadata=self._meta_v1_api(
                name=podname,
                namespace=session,
                labels={f"creator.{self.drunc_label}": self.__class__.__name__},
                owner_references=[
                    client.V1OwnerReference(
                        api_version="v1",
                        kind="Pod",
                        name=podname,
                        uid=pod_uid,
                        controller=True,
                        block_owner_deletion=True,
                    )
                ],
            ),
            spec=client.V1ServiceSpec(
                cluster_ip="None",
                selector={"app": podname},
                ports=[client.V1ServicePort(port=80, target_port=80)],
            ),
        )
        try:
            self._core_v1_api.create_namespaced_service(
                namespace=session, body=service_manifest
            )
            self.log.info(f'Created headless service "{session}.{podname}"')
        except self._api_error_v1_api as e:
            if e.status != 409:
                self.log.error(f"Failed to create headless service for {podname}: {e}")

    def _extract_port_from_cmd(self, boot_request: BootRequest) -> Optional[int]:
        """
        Extracts the port number from a BootRequest.

        - For LCS: Prioritizes the 'CONNECTION_PORT' environment variable.
        - For Controllers: Parses command arguments (e.g., '--port' or '-c grpc://...').
        """
        log = self.log
        app_name = boot_request.process_description.metadata.name
        log.debug(f"Attempting to extract port for '{app_name}'...")

        # --- PRIORITY 1: Check Environment Variables (for LCS) ---
        if app_name == self.connection_server_name:
            log.debug(f"'{app_name}' is LCS, checking 'CONNECTION_PORT' env var first.")
            env_vars = boot_request.process_description.env
            if "CONNECTION_PORT" in env_vars:
                port_str = env_vars["CONNECTION_PORT"]
                try:
                    port = int(port_str)
                    if port != 0:
                        log.info(
                            f"Extracted port {port} from 'CONNECTION_PORT' env var for LCS."
                        )
                        return port
                except (ValueError, TypeError):
                    log.error(
                        f"'CONNECTION_PORT' env var '{port_str}' is not valid int for LCS."
                    )
            else:
                log.debug(
                    "'CONNECTION_PORT' not found in env vars for LCS. Falling back to command parsing."
                )

        # --- PRIORITY 2: Parse Command Arguments (for Controllers or LCS fallback) ---
        log.debug(f"Parsing command arguments for '{app_name}'...")
        for i, e_and_a in enumerate(
            boot_request.process_description.executable_and_arguments
        ):
            all_args = [e_and_a.exec] + list(e_and_a.args)
            arg_str = " ".join(all_args)
            log.debug(f"Checking arg string part {i}: '{arg_str}'")

            # A. Check for gunicorn bind syntax (LCS fallback)
            if app_name == self.connection_server_name and "gunicorn" in arg_str:
                log.debug("Attempting gunicorn regex match for numeric port...")
                match = re.search(r"-b\s+[\w\.-]+:(\d+)", arg_str) or re.search(
                    r"--bind[\s=]+[\w\.-]+:(\d+)", arg_str
                )
                if match:
                    port = int(match.group(1))
                    if port != 0:
                        log.info(
                            f"Extracted gunicorn numeric port {port} from command arguments for LCS."
                        )
                        return port

            # B. Check for drunc-controller --port syntax (Controllers ONLY)
            if (
                app_name != self.connection_server_name
                and "controller" in arg_str
                and "drunc-controller" in e_and_a.exec
            ):
                log.debug("Attempting '--port' regex match for controller...")
                match_port = re.search(r"--port[\s=]+(\d+)", arg_str)
                if match_port:
                    port = int(match_port.group(1))
                    if port != 0:
                        log.info(
                            f"Extracted controller '--port {port}' from command args for '{app_name}'."
                        )
                        return port

            # C. Check for drunc-controller -c grpc://... syntax (Controllers ONLY)
            if (
                app_name != self.connection_server_name
                and "controller" in arg_str
                and "drunc-controller" in e_and_a.exec
            ):
                log.debug("Attempting '-c grpc://' regex match for controller...")
                match_c = re.search(r"-c\s+[\"\']?grpc:\/\/[^:]+:(\d+)[\"\']?", arg_str)
                if match_c:
                    port = int(match_c.group(1))
                    if port != 0:
                        log.info(
                            f"Extracted controller gRPC '-c ...:{port}' from command args for '{app_name}'."
                        )
                        return port

        log.error(f"Could not extract a valid non-zero port for '{app_name}'.")
        return None

    def _build_main_command(
        self, exec_and_args_list: List[ProcessDescription.ExecAndArgs], podname: str
    ) -> str:
        """
        Build the main command string from executable and arguments list.
        - Prepends 'exec' for Python apps to ensure they become PID 1.
        - Rewrites server addresses (e.g., 'grpc://localhost:port') to
          'protocol://0.0.0.0:port' to force binding to all interfaces
          within the container.
        """
        command_parts = []

        for i, e_and_a in enumerate(exec_and_args_list):
            is_last_command = i == len(exec_and_args_list) - 1

            # This is the "exec" fix for python apps (controllers, LCS)
            is_py_app = (
                e_and_a.exec != "source" and e_and_a.exec != "daq_application"
            )  # Assumes daq_application is C++
            prefix = "exec " if is_last_command and is_py_app else ""

            # Check if this is an app that starts a server
            is_server_app = (
                "controller" in podname
                or podname == self.connection_server_name
                or "daq_application" in e_and_a.exec
            )

            if is_server_app:
                modified_args = []
                # Regex to find 'protocol://hostname:port'
                # It allows 'localhost', IPs, or hostnames
                addr_regex = r"(grpc|rest|http|https):\/\/(localhost|[\w\.-]+|127\.0\.0\.1):(\d+)"

                for arg in e_and_a.args:
                    # Replace 'hostname' with '0.0.0.0'
                    new_arg, count = re.subn(addr_regex, r"\1://0.0.0.0:\3", arg)

                    if count > 0:
                        self.log.info(
                            f"Rewriting server bind address for '{podname}': '{arg}' -> '{new_arg}'"
                        )
                        modified_args.append(new_arg)
                    else:
                        modified_args.append(arg)

                command_parts.append(prefix + " ".join([e_and_a.exec] + modified_args))
            else:
                # For other pods (like 'echo'), use arguments as-is
                command_parts.append(
                    prefix + " ".join([e_and_a.exec] + list(e_and_a.args))
                )

        return " && ".join(command_parts)

    def _build_main_container(
        self,
        podname: str,
        pod_image: str,
        main_command_str: str,
        boot_request: BootRequest,
        controller_port: Optional[int],
    ) -> client.V1Container:
        """Builds the V1Container spec for the main application container."""

        # --- Lifecycle hook (for graceful shutdown) ---
        shutdown_command = ""
        if "controller" in podname or podname == self.connection_server_name:
            # Python apps: send SIGINT
            shutdown_command = """
for p in /proc/[0-9]*; do
  if [ -f "$p/cmdline" ] && grep -Ea "(drunc-controller|gunicorn).*:drunc.plugins.local_connectivity_server.app" "$p/cmdline" > /dev/null; then
    kill -SIGINT $(basename "$p");
  fi
done
"""
        else:  # C++ Applications: send SIGQUIT
            shutdown_command = "kill -QUIT 1"

        lifecycle_hook = client.V1Lifecycle(
            pre_stop=client.V1LifecycleHandler(
                _exec=client.V1ExecAction(command=["/bin/sh", "-c", shutdown_command])
            )
        )

        # --- Configure HostPort if root-controller ---
        container_ports = []
        if "root-controller" in podname and controller_port:
            container_ports = [
                client.V1ContainerPort(
                    container_port=controller_port,
                    host_port=controller_port,  # Map node's port directly to container's port
                    name="grpc",
                    protocol="TCP",
                )
            ]
            self.log.info(
                f"Configuring HostPort {controller_port} TCP for '{podname}'."
            )
        elif "root-controller" in podname:
            self.log.warning(
                f"Root controller '{podname}' has no valid port; cannot configure HostPort."
            )

        # --- Assemble Container Spec ---
        return client.V1Container(
            name=podname,
            image=pod_image,
            command=["/bin/sh", "-c"],
            args=[main_command_str],
            env=[
                client.V1EnvVar(name=k, value=v)
                for k, v in boot_request.process_description.env.items()
            ],
            lifecycle=lifecycle_hook,
            ports=container_ports,  # Use the port list (empty unless root-controller)
            volume_mounts=[
                client.V1VolumeMount(name="nfs", mount_path="/nfs"),
                client.V1VolumeMount(name="cvmfs", mount_path="/cvmfs"),
            ],
            working_dir=boot_request.process_description.process_execution_directory,
            security_context=client.V1SecurityContext(
                run_as_user=os.getuid(), run_as_group=os.getgid()
            ),
        )

    def _get_node_selector(
        self, boot_request: BootRequest
    ) -> Tuple[Dict[str, str], Optional[str]]:
        """Determines node selector and returns the target_host if specified."""
        if not boot_request.process_restriction.allowed_hosts:
            return {}, None

        target_host = boot_request.process_restriction.allowed_hosts[0]
        if target_host == "localhost":
            target_host = resolve_localhost_to_hostname(target_host)
            self.log.info(f"Resolved localhost to '{target_host}' for node selection")

        self._verify_host_in_cluster(target_host)
        node_selector = {"kubernetes.io/hostname": target_host}
        self.log.info(f"Pod will be scheduled on node '{target_host}'")
        return node_selector, target_host

    def _get_host_aliases(self, podname: str, session: str) -> List[client.V1HostAlias]:
        """Builds host aliases for mapping 'localhost' to the LCS ClusterIP."""
        if (
            podname == self.connection_server_name
            or not self.local_connection_server_is_booted
        ):
            return []

        connection_server_ip = self._get_connection_server_cluster_ip(session)
        if connection_server_ip:
            self.log.info(
                f"Pod '{podname}' will resolve localhost to LCS ClusterIP {connection_server_ip}"
            )
            return [
                client.V1HostAlias(ip=connection_server_ip, hostnames=["localhost"])
            ]

        self.log.warning(f"Could not get LCS ClusterIP for pod '{podname}' hostAlias.")
        return []

    def _build_pod_manifest(
        self, podname: str, session: str, boot_request: BootRequest, port: Optional[int]
    ) -> client.V1Pod:
        """Builds the complete V1Pod manifest object."""

        main_command_str = self._build_main_command(
            boot_request.process_description.executable_and_arguments, podname
        )

        main_container = self._build_main_container(
            podname, self.configuration.data.image, main_command_str, boot_request, port
        )
        node_selector, _ = self._get_node_selector(boot_request)
        host_aliases = self._get_host_aliases(podname, session)

        return client.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=self._meta_v1_api(
                name=podname,
                namespace=session,
                labels={
                    "app": podname,
                    f"creator.{self.drunc_label}": self.__class__.__name__,
                },
            ),
            spec=self._pod_spec_v1_api(
                node_selector=node_selector,
                termination_grace_period_seconds=self.kill_timeout,
                restart_policy="Never",
                containers=[main_container],
                host_aliases=host_aliases if host_aliases else None,
                volumes=[
                    client.V1Volume(
                        name="nfs", host_path=client.V1HostPathVolumeSource(path="/nfs")
                    ),
                    client.V1Volume(
                        name="cvmfs",
                        host_path=client.V1HostPathVolumeSource(path="/cvmfs"),
                    ),
                ],
            ),
        )

    def _create_services_for_pod(
        self, podname: str, session: str, pod_uid: str, port: Optional[int]
    ) -> None:
        """Creates the necessary K8s Service for a given pod."""

        is_root_controller = "root-controller" in podname

        if podname == self.connection_server_name:
            if port:
                # Class variables (self.connection_server_port) set in __boot
                self._create_nodeport_service(podname, session, pod_uid)
            else:
                self.log.error(
                    f"Cannot create NodePort service for LCS '{podname}', port is missing."
                )

        elif is_root_controller:
            self.log.info(
                f"Skipping Service creation for '{podname}' as HostPort is configured."
            )

        else:
            # Create headless service for all other pods for internal DNS
            self._create_headless_service(podname, session, pod_uid)

    def _create_pod(
        self, podname: str, session: str, boot_request: BootRequest, port: Optional[int]
    ) -> str:
        """
        Orchestrates the creation of the Pod manifest, the Pod resource, and any associated Services.
        Returns the UID of the created pod.
        """

        pod_manifest = self._build_pod_manifest(podname, session, boot_request, port)

        start_time = time()
        try:
            while True:
                try:
                    created_pod = self._core_v1_api.create_namespaced_pod(
                        session, pod_manifest
                    )
                    self.log.info(f'Creating pod "{session}.{podname}"')
                    pod_uid = created_pod.metadata.uid

                    self._create_services_for_pod(podname, session, pod_uid, port)

                    return pod_uid  # Success

                except self._api_error_v1_api as e:
                    if e.status == 409 and (
                        time() - start_time < self.restart_cleanup_time
                    ):
                        sleep(self.restart_cleanup_polling)
                        continue
                    else:
                        raise

        except self._api_error_v1_api as e:
            error_message = f'Couldn\'t create resources for pod "{session}.{podname}". Reason: {e.reason}.'
            if e.status == 409:
                error_message = f"Timeout (>{self.restart_cleanup_time}s) waiting for old pod '{session}/{podname}' to be deleted."
            self.log.error(error_message)
            raise DruncK8sException(error_message) from e
        except Exception as e:
            self.log.exception(
                f"Unexpected error during pod/service creation for '{podname}': {e}"
            )
            raise DruncK8sException(f"Unexpected error creating pod {podname}") from e

    def _wait_for_pod_api_ready(self, podname: str, session: str) -> str:
        """
        [Stage 1 Wait] Waits for a pod to be 'Running' and 'Ready' in the K8s API.

        Returns:
            str: The name of the node the pod is scheduled on.
        """
        self.log.info(
            f"Stage 1: Waiting for '{podname}' pod to be Running and Ready..."
        )
        start_time = time()
        pod_ready = False
        node_name = None

        while not pod_ready and (time() - start_time < self.pod_ready_timeout):
            try:
                pod_status = self._core_v1_api.read_namespaced_pod_status(
                    podname, session
                )

                if pod_status.status.phase == "Failed":
                    raise DruncK8sPodException(
                        f"Pod '{podname}' failed to start. Check pod logs."
                    )

                if pod_status.status.phase == "Running" and pod_status.status.pod_ip:
                    is_ready = False
                    if pod_status.status.conditions:
                        for condition in pod_status.status.conditions:
                            if condition.type == "Ready" and condition.status == "True":
                                is_ready = True
                                break
                    if is_ready:
                        self.log.info(
                            f"Stage 1: Pod '{podname}' is API Ready with IP {pod_status.status.pod_ip}."
                        )
                        node_name = pod_status.spec.node_name
                        pod_ready = True

            except self._api_error_v1_api as e:
                if e.status == 404:
                    pass  # Pod not yet visible
                else:
                    raise
            except DruncK8sPodException:
                raise  # Re-raise pod failure
            except Exception as e:
                self.log.warning(f"Error checking pod status for '{podname}': {e}")

            sleep(self.pod_status_check_sleep)

        if not pod_ready:
            raise DruncK8sException(
                f"'{podname}' pod did not become API Ready in {self.pod_ready_timeout} seconds."
            )

        if not node_name:
            raise DruncK8sException(f"Could not determine node name for '{podname}'.")

        return node_name

    def _wait_for_http_port_ready(self, node_name: str, port: int) -> None:
        """[Stage 2 Wait] Waits for a URL to be reachable via HTTP (for LCS)."""

        self.log.info(
            f"Stage 2: Waiting for NodePort (HTTP) http://{node_name}:{port} to be reachable..."
        )
        port_ready = False
        url = f"http://{node_name}:{port}"

        start_time = time()
        http_startup_timeout = self.pod_ready_timeout

        while not port_ready and (time() - start_time < http_startup_timeout):
            try:
                urllib.request.urlopen(url, timeout=1)
                port_ready = True
            except (
                urllib.error.URLError,
                ConnectionRefusedError,
                TimeoutError,
                OSError,
            ) as e:
                self.log.debug(f"NodePort {url} not ready yet ({e}), retrying...")
                sleep(self.pod_status_check_sleep)

        if not port_ready:
            raise DruncK8sException(
                f"NodePort {url} did not become reachable in {http_startup_timeout} seconds."
            )

        self.log.info(f"Stage 2: NodePort {url} is now active.")

    def _wait_for_tcp_port_ready(self, node_name: str, port: int) -> None:
        """[Stage 2 Wait] Waits for a port to be reachable via TCP (for gRPC controllers)."""

        self.log.info(
            f"Stage 2: Waiting for HostPort (TCP) {node_name}:{port} to be reachable..."
        )
        port_ready = False

        grpc_startup_timeout = 120  # Wait up to 2 minutes
        start_time = time()

        while not port_ready and (time() - start_time < grpc_startup_timeout):
            sock = None
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1.0)
                result = sock.connect_ex((node_name, port))

                if result == 0:
                    port_ready = True
                else:
                    self.log.debug(
                        f"HostPort {node_name}:{port} not ready yet (socket error {result}), retrying..."
                    )
                    sleep(self.pod_status_check_sleep)

            except socket.gaierror as e:
                self.log.warning(
                    f"Failed to resolve hostname '{node_name}': {e}. Retrying..."
                )
                sleep(self.pod_status_check_sleep)
            except Exception as e:
                self.log.debug(
                    f"HostPort not ready yet (Socket error: {e}), retrying..."
                )
                sleep(self.pod_status_check_sleep)
            finally:
                if sock:
                    sock.close()

        if not port_ready:
            raise DruncK8sException(
                f"HostPort {node_name}:{port} did not become reachable in {grpc_startup_timeout} seconds."
            )

        self.log.info(
            f"Stage 2: HostPort {node_name}:{port} is active (TCP connect success)."
        )

    def _check_nodeport_collision(self, port: int, session: str, podname: str) -> None:
        """Checks if the requested NodePort is already in use."""
        self.log.debug(f"Checking for NodePort collisions on port {port}")
        api = self._core_v1_api
        all_services = api.list_service_for_all_namespaces()
        for svc in all_services.items:
            if not svc.spec.type == "NodePort":
                continue
            for p in svc.spec.ports:
                if p.node_port == port and (
                    svc.metadata.namespace != session or svc.metadata.name != podname
                ):
                    raise DruncK8sException(
                        f"NodePort {port} is already in use by service "
                        f"{svc.metadata.name} in namespace {svc.metadata.namespace}. "
                        "Cannot start another local connection server with the same port."
                    )

    def _get_process_uid(self, query: ProcessQuery, order_by: str = None) -> list[str]:
        """
        Finds process UUIDs matching a query.

        If order_by is "leaf_first", it sorts the UUIDs so that child processes
        (which have a longer tree_id) come before their parents.
        """
        initial_match = set()
        for proc_uuid, boot_req in self.boot_request.items():
            meta = boot_req.process_description.metadata
            query_is_empty = not any(
                [query.uuids, query.names, query.session, query.user]
            )

            if (
                query_is_empty
                or any(uid.uuid == proc_uuid for uid in query.uuids)
                or (query.session and query.session == meta.session)
                or (query.user and query.user == meta.user)
                or any(re.search(name_reg, meta.name) for name_reg in query.names)
            ):
                initial_match.add(proc_uuid)

        if order_by != "leaf_first":
            return list(initial_match)

        self.log.debug("Sorting processes in leaf-first order using tree_id.")

        procs_to_sort = []
        for a_uuid in initial_match:
            if a_uuid in self.boot_request:
                tree_id = self.boot_request[a_uuid].process_description.metadata.tree_id
                procs_to_sort.append((a_uuid, tree_id))

        procs_to_sort.sort(key=lambda p: (-len(p[1]), p[1]))
        sorted_uuids = [uuid for uuid, tree_id in procs_to_sort]
        return sorted_uuids

    def _logs_impl(self, log_request: LogRequest) -> LogLines:
        """Handles the 'logs' command."""
        uuids = self._get_process_uid(log_request.query)
        uuid = self._ensure_one_process(uuids, in_boot_request=True)
        podname = self.boot_request[uuid].process_description.metadata.name
        session = self.boot_request[uuid].process_description.metadata.session
        try:
            logs = self._core_v1_api.read_namespaced_pod_log(
                podname, session, tail_lines=log_request.how_far or 100
            )
            return LogLines(uuid=ProcessUUID(uuid=uuid), lines=logs.split("\n"))
        except self._api_error_v1_api as e:
            return LogLines(
                uuid=ProcessUUID(uuid=uuid),
                lines=[f"Could not retrieve logs: {e.reason}"],
            )

    def _boot_impl(self, boot_request: BootRequest) -> ProcessInstanceList:
        """Handles the 'boot' command from the gRPC interface."""
        self.log.debug(f"{self.name} running boot command")
        this_uuid = str(uuid.uuid4())
        process = self.__boot(boot_request, this_uuid)
        return ProcessInstanceList(values=[process])

    def __boot(self, boot_request: BootRequest, uuid: str) -> ProcessInstance:
        """
        Internal boot method. Handles pod creation and waits for critical services.
        - Waits for LCS (NodePort) and root-controller (HostPort) to be fully ready.
        - Non-blocking for all other pods.
        """
        session = boot_request.process_description.metadata.session
        podname = boot_request.process_description.metadata.name
        self.log.debug(f"__boot called for '{podname}' (UUID {uuid})")

        # Validation and Pre-checks
        if not validate_k8s_session_name(session):
            raise DruncK8sNamespaceException(
                f'Invalid session/namespace name "{session}". Must match RFC1123 label.'
            )

        if boot_request.process_restriction.allowed_hosts:
            hostname = boot_request.process_restriction.allowed_hosts[0]
            boot_request.process_description.metadata.hostname = hostname

        if uuid in self.boot_request:
            raise DruncK8sPodException(f'"{session}.{podname}":{uuid} already exists!')

        port = self._extract_port_from_cmd(boot_request)

        # LCS-specific Pre-boot Checks
        if podname == self.connection_server_name:
            if not port:
                raise DruncK8sException(
                    f"Cannot boot LCS '{podname}', port extraction failed."
                )
            self.connection_server_port = port
            self.connection_server_node_port = port
            self._check_nodeport_collision(port, session, podname)

        # Create K8s Resources
        self._create_namespace(session)

        self.boot_request[uuid] = BootRequest()
        self.boot_request[uuid].CopyFrom(boot_request)

        # Create Pod, Services, and get UID
        self._create_pod(podname, session, boot_request, port)
        # Add the UUID label *after* the pod is successfully created
        self._add_label(podname, "pod", "uuid", uuid, session=session)
        self.log.info(f'"{session}.{podname}":{uuid} boot request sent.')

        # Wait for Critical Pods (LCS and root-controller)
        try:
            if podname == self.connection_server_name:
                node_name = self._wait_for_pod_api_ready(podname, session)
                self._wait_for_http_port_ready(node_name, port)
                self.local_connection_server_is_booted = True
                self.log.info(f"Connection server '{podname}' is fully ready.")

            elif "root-controller" in podname:
                node_name = self._wait_for_pod_api_ready(podname, session)
                self._wait_for_tcp_port_ready(node_name, port)
                self.log.info(f"Controller '{podname}' is fully ready.")

        except Exception as e:
            # If the wait fails, we need to clean up the pod we just created
            self.log.error(
                f"Failed to wait for pod '{podname}' to be ready: {e}. Attempting cleanup..."
            )
            try:
                self._kill_pod(podname, session)  # Use the simple kill pod method
            except Exception as kill_e:
                self.log.error(
                    f"Failed to cleanup pod '{podname}' after wait failure: {kill_e}"
                )
            raise  # Re-raise the original waiting exception

        # Return ProcessInstance
        pd, pr, pu = ProcessDescription(), ProcessRestriction(), ProcessUUID(uuid=uuid)
        pd.CopyFrom(self.boot_request[uuid].process_description)
        pr.CopyFrom(self.boot_request[uuid].process_restriction)

        return ProcessInstance(
            process_description=pd,
            process_restriction=pr,
            status_code=ProcessInstance.StatusCode.RUNNING,
            uuid=pu,
        )

    def _ps_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        """Handles the 'ps' command."""
        queried_uuids = self._get_process_uid(query)
        if not queried_uuids:
            return ProcessInstanceList(values=[])

        pod_list = None
        if query.session:
            pod_list = self._core_v1_api.list_namespaced_pod(
                namespace=query.session,
                label_selector=self._get_creator_label_selector(),
            )
        else:
            pod_list = self._core_v1_api.list_pod_for_all_namespaces(
                label_selector=self._get_creator_label_selector()
            )

        uuid_to_pod = {
            p.metadata.labels.get(f"uuid.{self.drunc_label}"): p for p in pod_list.items
        }
        ret = []
        for proc_uuid in queried_uuids:
            if proc_uuid not in self.boot_request:
                continue
            pod = uuid_to_pod.get(proc_uuid)
            status_code = ProcessInstance.StatusCode.DEAD
            return_code = None
            if pod:
                if pod.status.phase == "Running":
                    status_code = ProcessInstance.StatusCode.RUNNING
                elif pod.status.phase in ["Succeeded", "Failed"]:
                    if (
                        pod.status.container_statuses
                        and pod.status.container_statuses[0].state.terminated
                    ):
                        return_code = pod.status.container_statuses[
                            0
                        ].state.terminated.exit_code
            pd, pr, pu = (
                ProcessDescription(),
                ProcessRestriction(),
                ProcessUUID(uuid=proc_uuid),
            )
            pd.CopyFrom(self.boot_request[proc_uuid].process_description)
            pr.CopyFrom(self.boot_request[proc_uuid].process_restriction)
            ret.append(
                ProcessInstance(
                    process_description=pd,
                    process_restriction=pr,
                    status_code=status_code,
                    return_code=return_code,
                    uuid=pu,
                )
            )
        return ProcessInstanceList(values=ret)

    def _restart_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        """Handles the 'restart' command."""
        uuids = self._get_process_uid(query)
        uuid = self._ensure_one_process(uuids, in_boot_request=True)

        if uuid not in self.boot_request:
            raise DruncK8sPodException(
                f"Cannot restart process with UUID {uuid}: Not found."
            )

        br_copy = BootRequest()
        br_copy.CopyFrom(self.boot_request[uuid])

        kill_query = ProcessQuery(uuids=[ProcessUUID(uuid=uuid)])
        self._kill_impl(kill_query)

        restarted_process = self.__boot(br_copy, uuid)
        return ProcessInstanceList(values=[restarted_process])

    def _kill_pod(self, podname, session, grace_period_seconds=None) -> None:
        """Deletes a specific pod from a namespace."""
        try:
            self._core_v1_api.delete_namespaced_pod(
                name=podname,
                namespace=session,
                grace_period_seconds=grace_period_seconds,
            )
        except self._api_error_v1_api as e:
            if e.status != 404:
                raise DruncK8sException(
                    f"Failed to delete pod '{session}.{podname}': {e}"
                )

    def _kill_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        """Handles the 'kill' command."""
        uuids_to_kill = self._get_process_uid(query, order_by="leaf_first")
        if not uuids_to_kill:
            return ProcessInstanceList(values=[])

        self.log.info(f"Starting termination of {len(uuids_to_kill)} pods...")

        graceful_apps, forced_apps = [], []
        for uuid_str in uuids_to_kill:
            if uuid_str not in self.boot_request:
                continue

            pd = self.boot_request[uuid_str].process_description
            is_controller = (
                "controller" in pd.metadata.name
                or pd.metadata.name == self.connection_server_name
            )

            if is_controller:
                forced_apps.append(uuid_str)
            else:
                graceful_apps.append(uuid_str)

        def kill_and_wait(uuids, stage_name, grace_period=None) -> None:
            if not uuids:
                return
            action = (
                "Forcing termination of"
                if grace_period == 0
                else "Gracefully terminating"
            )
            self.log.info(f"Stage '{stage_name}': {action} {len(uuids)} pod(s)...")

            self.termination_complete_event.clear()
            self.uuids_pending_deletion.update(uuids)

            for proc_uuid in uuids:
                if proc_uuid not in self.boot_request:
                    continue  # Sanity check
                pd = self.boot_request[proc_uuid].process_description
                self.log.info(
                    f'Killing pod "{pd.metadata.session}/{pd.metadata.name}" (UUID {proc_uuid})'
                )
                self._kill_pod(
                    pd.metadata.name,
                    pd.metadata.session,
                    grace_period_seconds=grace_period,
                )

            wait_timeout = self.kill_timeout if grace_period is None else 15
            if not self.termination_complete_event.wait(timeout=wait_timeout):
                self.log.warning(
                    f"Timeout in stage '{stage_name}'. Remaining: {self.uuids_pending_deletion}"
                )

            self.uuids_pending_deletion.clear()

        kill_and_wait(graceful_apps, "Standalone C++ Applications")

        kill_and_wait(forced_apps, "Controllers & Local Session Apps", grace_period=0)

        final_ret = []
        for proc_uuid in uuids_to_kill:
            if proc_uuid in self.boot_request:
                pi = ProcessInstance(
                    process_description=self.boot_request[
                        proc_uuid
                    ].process_description,
                    process_restriction=self.boot_request[
                        proc_uuid
                    ].process_restriction,
                    status_code=ProcessInstance.StatusCode.DEAD,
                    uuid=ProcessUUID(uuid=proc_uuid),
                    return_code=self.final_exit_codes.get(proc_uuid, -1),
                )
                final_ret.append(pi)
                del self.boot_request[proc_uuid]

        # If our internal process list is empty, we can clean up the namespace we used.
        if not self.boot_request:
            self.log.info(
                "All tracked processes terminated. Cleaning up managed namespace..."
            )
            try:
                for session in list(self.managed_sessions):
                    self.log.info(f'Session "{session}" is empty, deleting namespace.')
                    self._core_v1_api.delete_namespace(session)
                    self.managed_sessions.remove(session)

            except self._api_error_v1_api as e:
                self.log.warning(f"Failed during namespace cleanup: {e}")

        return ProcessInstanceList(values=final_ret)

    def _terminate_impl(self) -> ProcessInstanceList:
        """Handles the 'terminate' command, killing all known processes."""
        self.log.info("Terminating all known K8s processes.")
        if not self.boot_request:
            self.log.info("No processes to terminate.")
            return ProcessInstanceList(values=[])
        all_processes_query = ProcessQuery(names=[".*"])
        return self._kill_impl(all_processes_query)

    def _flush_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        """Handles the 'flush' command (no-op for Kubernetes)."""
        self.log.info(
            "The 'flush' command is not needed for the K8sProcessManager. "
            "Cleanup of dead processes is handled automatically in real-time."
        )
        return ProcessInstanceList(values=[])
