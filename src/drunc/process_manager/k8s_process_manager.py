# Standard Library Imports
import getpass
import os
import re
import signal
import socket
import threading
import urllib.error
import urllib.request
import uuid
from time import sleep, time

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
from drunc.process_manager.utils import on_parent_exit, validate_k8s_session_name
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

                        self.pm.log.debug(
                            f"Pod {proc_uuid} terminated: phase={phase}, is_terminal={is_terminal_phase}, is_deleted={is_deleted_event}"
                        )
                        if phase == "Succeeded":
                            exit_code = 0
                            reason = "GracefulShutdown"
                        elif (
                            status.container_statuses
                            and status.container_statuses[0].state.terminated
                        ):
                            terminated_state = status.container_statuses[
                                0
                            ].state.terminated
                            exit_code = terminated_state.exit_code
                            reason = (
                                terminated_state.reason
                            )  # Finally, handle deleted events
                        elif is_deleted_event:
                            if phase == "Succeeded":
                                exit_code = 0
                                reason = "GracefulShutdown"
                            else:
                                exit_code = -1
                                reason = "PodDeleted"

                        self.pm.log.debug(
                            f"Final result for pod {proc_uuid}: exit_code={exit_code}, reason={reason}"
                        )

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
        self.kill_timeout = pod_management.get("kill_timeout", 30)
        self.pod_ready_timeout = pod_management.get("pod_ready_timeout", 60)
        self.total_shutdown_timeout = pod_management.get("total_shutdown_timeout", 60)

        # Volume mounts
        self.volume_configs = settings.get("volumes", [])

        # Cleanup
        cleanup = settings.get("cleanup", {})
        self.restart_cleanup_time = cleanup.get("restart_cleanup_time", 10.0)
        self.restart_cleanup_polling = cleanup.get("restart_cleanup_polling", 0.5)

        # Checking
        checking = settings.get("checking", {})
        self.watcher_retry_sleep = checking.get("watcher_retry_sleep", 5)
        self.pod_status_check_sleep = checking.get("pod_status_check_sleep", 1)
        self._host_cache_expiry = checking.get("host_cache_expiry", 300)
        self.grpc_startup_timeout = checking.get("grpc_startup_timeout", 30)
        self.socket_retry_timeout = checking.get("socket_retry_timeout", 1.0)

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

        # Set up signal handlers for cleanup when parent process dies
        self._setup_signal_handlers()

    def _start_watcher(self) -> None:
        """Starts the background thread that watches for Pod status changes."""
        self.log.debug("Starting K8s pod watcher thread")
        t = K8sPodWatcherThread(pm=self)
        t.start()
        self.watchers.append(t)

    def _setup_signal_handlers(self) -> None:
        """Set up signal handlers to clean up pods when the process manager is terminated."""

        def signal_handler(signum, frame):
            self.log.info(f"Received signal {signum}, cleaning up all pods...")
            try:
                self._terminate_impl()
            except Exception as e:
                self.log.error(f"Error during signal cleanup: {e}")
            finally:
                # Exit the process
                os._exit(0)

        # Register signal handlers for common termination signals
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGHUP, signal_handler)
        signal.signal(signal.SIGQUIT, signal_handler)

        # Set up parent death signal (Linux only)
        try:
            on_parent_exit(signal.SIGTERM)()
        except Exception as e:
            self.log.debug(
                f"Could not set parent death signal (may not be supported on this platform): {e}"
            )

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

    def _create_nodeport_service(self, podname, session, pod_uid) -> None:
        """Creates a NodePort service for the connection server (external + internal access)."""
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
                type="NodePort",
                external_traffic_policy="Local",
                selector={"app": podname},
                ports=[
                    client.V1ServicePort(
                        protocol="TCP",
                        port=self.connection_server_port,
                        target_port=self.connection_server_port,
                        node_port=self.connection_server_node_port,
                    )
                ],
            ),
        )
        try:
            self._core_v1_api.create_namespaced_service(
                namespace=session, body=service_manifest
            )
            self.log.info(
                f'Created NodePort service "{session}.{podname}" on port {self.connection_server_port} '
                f"(NodePort: {self.connection_server_node_port} for external access)"
            )

        except self._api_error_v1_api as e:
            is_port_conflict = False

            # Check for 422="Unprocessable Entity" or 409="Conflict" status
            if e.status == 422 or e.status == 409:
                if e.body and (
                    "provided nodeport is already allocated" in e.body.lower()
                    or "port is already in use" in e.body.lower()
                ):
                    is_port_conflict = True

            if is_port_conflict:
                port = self.connection_server_node_port
                error_message = (
                    f"NodePort {port} is already in use by another service. "
                    f"Cannot start '{podname}'."
                )
                self.log.error(error_message)
                raise DruncK8sException(error_message) from e
            else:
                # other K8s API error
                error_message = f"Failed to create NodePort service for {podname}: {e.reason} ({e.status})"
                self.log.error(error_message)
                raise DruncK8sException(error_message) from e

    def _build_pod_main_container(
        self, podname: str, boot_request: BootRequest, lcs_port: int | None
    ) -> client.V1Container:
        """Builds the primary V1Container manifest, including command and preStop hook."""

        pod_image = self.configuration.data.image
        exec_and_args_list = boot_request.process_description.executable_and_arguments

        # This logic correctly prepends 'exec' to the C++ application command.
        command_parts = []
        for i, e_and_a in enumerate(exec_and_args_list):
            is_last_command = i == len(exec_and_args_list) - 1
            prefix = ""
            # Only add 'exec' to the C++ apps (non-controllers)
            if (
                "controller" not in podname
                and podname != self.connection_server_name
                and is_last_command
                and e_and_a.exec != "source"
            ):
                prefix = "exec "

            command_parts.append(prefix + " ".join([e_and_a.exec] + list(e_and_a.args)))
        main_command_str = " && ".join(command_parts)

        container_ports = []
        if podname == self.connection_server_name and lcs_port is not None:
            container_ports.append(
                client.V1ContainerPort(container_port=lcs_port, name="http-port")
            )

        # Only add preStop hook for C++ applications (non-controllers)
        lifecycle_hook = None
        if "controller" not in podname and podname != self.connection_server_name:
            self.log.debug(
                f"'{podname}' identified as a C++ app, adding preStop hook with SIGQUIT."
            )
            shutdown_command = "kill -QUIT 1"
            lifecycle_hook = client.V1Lifecycle(
                pre_stop=client.V1LifecycleHandler(
                    _exec=client.V1ExecAction(
                        command=["/bin/sh", "-c", shutdown_command]
                    )
                )
            )
        else:
            self.log.debug(
                f"'{podname}' identified as a Python app, no preStop hook needed."
            )

        # Prepare mounts
        container_volume_mounts = [
            client.V1VolumeMount(
                name=vc["name"],
                mount_path=vc["mount_path"],
                read_only=vc.get("read_only", True),
            )
            for vc in self.volume_configs
        ]

        main_container = client.V1Container(
            name=podname,
            image=pod_image,
            command=["/bin/sh", "-c"],
            args=[main_command_str],
            env=[
                client.V1EnvVar(name=k, value=v)
                for k, v in boot_request.process_description.env.items()
            ],
            lifecycle=lifecycle_hook,
            ports=container_ports,
            volume_mounts=container_volume_mounts,
            working_dir=boot_request.process_description.process_execution_directory,
            security_context=client.V1SecurityContext(
                run_as_user=os.getuid(), run_as_group=os.getgid()
            ),
        )
        return main_container

    def _get_pod_node_selector(
        self, podname: str, restriction: ProcessRestriction
    ) -> dict:
        """Verifies the target host and returns the Kubernetes node selector."""
        node_selector = {}
        if restriction.allowed_hosts:
            target_host = restriction.allowed_hosts[0]

            if target_host == "localhost":
                target_host = resolve_localhost_to_hostname(target_host)
                self.log.info(
                    f"Resolved localhost to '{target_host}' for node selection"
                )

            self._verify_host_in_cluster(target_host)

            node_selector = {"kubernetes.io/hostname": target_host}
            self.log.info(
                f"Pod '{podname}' will be scheduled on node '{target_host}' (from boot request)"
            )
        return node_selector

    def _get_pod_host_aliases(
        self, podname: str, session: str
    ) -> list[client.V1HostAlias] | None:
        """Gets the ClusterIP of the connection server and prepares host aliases."""
        host_aliases = None
        if (
            podname != self.connection_server_name
            and self.local_connection_server_is_booted
        ):
            connection_server_ip = None
            retry_count = 0
            max_retries = 10
            while not connection_server_ip and retry_count < max_retries:
                connection_server_ip = self._get_connection_server_cluster_ip(session)
                if not connection_server_ip:
                    sleep(1)
                    retry_count += 1

            if connection_server_ip:
                host_aliases = [
                    client.V1HostAlias(ip=connection_server_ip, hostnames=["localhost"])
                ]
                self.log.info(
                    f"Pod '{podname}' will resolve localhost to connection server IP {connection_server_ip}"
                )
            else:
                self.log.warning(
                    f"Could not get connection server ClusterIP for pod '{podname}'"
                )
        return host_aliases

    def _build_pod_manifest(
        self,
        podname: str,
        session: str,
        main_container: client.V1Container,
        node_selector: dict,
        host_aliases: list[client.V1HostAlias] | None,
    ) -> client.V1Pod:
        """Assembles the final V1Pod object."""

        # Prepare mounts
        pod_volumes = [
            client.V1Volume(
                name=vc["name"],
                host_path=client.V1HostPathVolumeSource(
                    path=vc["host_path"], type="Directory"
                ),
            )
            for vc in self.volume_configs
        ]

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
                volumes=pod_volumes,
            ),
        )

    def _execute_pod_creation_api(
        self, session: str, podname: str, pod_manifest: client.V1Pod
    ) -> str:
        """Executes the API call to create the pod, handling 409 conflict during restarts."""
        start_time = time()

        while True:
            try:
                created_pod = self._core_v1_api.create_namespaced_pod(
                    session, pod_manifest
                )
                self.log.info(f'Creating pod "{session}.{podname}"')
                return created_pod.metadata.uid

            except self._api_error_v1_api as e:
                is_409_conflict = e.status == 409
                elapsed_time = time() - start_time

                if is_409_conflict and elapsed_time < self.restart_cleanup_time:
                    sleep(self.restart_cleanup_polling)
                    continue

                if is_409_conflict:
                    error_message = (
                        f"Timeout (>{self.restart_cleanup_time}s) waiting for old pod object "
                        f'"{session}/{podname}" to be fully deleted. Could not restart pod.'
                    )
                    self.log.error(error_message)
                    raise DruncK8sException(error_message) from e

                raise e

    def _create_associated_service(
        self,
        podname: str,
        session: str,
        pod_uid: str,
        boot_request: BootRequest,
        lcs_port: int | None,
    ) -> None:
        """Calls the appropriate service creation method based on pod type."""
        if podname == self.connection_server_name:
            if lcs_port is None:
                raise DruncK8sException(
                    "LCS service creation failed: port was not extracted."
                )

            # If LCS, call nodeport service creation
            self._create_nodeport_service(podname, session, pod_uid)

        elif "root-controller" in podname:
            self.log.info(
                f"'{podname}' is the root controller, checking for NodePort service."
            )
            port = self._extract_port_from_cmd(boot_request)
            if port:
                self.log.info(f"Extracted port {port} for '{podname}' NodePort.")
                self.connection_server_port = port
                self.connection_server_node_port = port
                self._create_nodeport_service(podname, session, pod_uid)
            else:
                self.log.warning(
                    f"Could not extract port for '{podname}', falling back to headless."
                )
                self._create_headless_service(podname, session, pod_uid)

        else:
            self._create_headless_service(podname, session, pod_uid)

    def _create_pod(self, podname, session, boot_request: BootRequest) -> None:
        """Constructs and creates a Kubernetes Pod manifest and its associated service."""
        try:
            lcs_port = None

            # Early Port Extraction and Class Variable Setup for LCS
            if podname == self.connection_server_name:
                lcs_port = self._extract_port_from_cmd(boot_request)
                if lcs_port:
                    self.connection_server_port = lcs_port
                    self.connection_server_node_port = lcs_port
                else:
                    raise DruncK8sException(
                        f"Could not extract port for LCS '{podname}'."
                    )

            # Build the main container manifest
            main_container = self._build_pod_main_container(
                podname, boot_request, lcs_port
            )

            # Node_selector, host_aliases, pod_manifest
            node_selector = self._get_pod_node_selector(
                podname, boot_request.process_restriction
            )
            host_aliases = self._get_pod_host_aliases(podname, session)
            pod_manifest = self._build_pod_manifest(
                podname,
                session,
                main_container,
                node_selector,
                host_aliases,
            )

            # Execute the pod creation API call
            pod_uid = self._execute_pod_creation_api(session, podname, pod_manifest)

            # Create associated service
            self._create_associated_service(
                podname, session, pod_uid, boot_request, lcs_port
            )

        except self._api_error_v1_api as e:
            # *other* K8s errors (e.g., 400, 403, 500)
            error_message = f'Couldn\'t create resources for pod "{session}.{podname}". Reason: {e.reason}. Kubernetes API Error: ({e.status})'
            self.log.error(error_message)
            raise DruncK8sException(error_message) from e

        except DruncK8sException:
            # any other DruncK8sException
            raise

        except Exception as e:
            # generic catch-all
            raise DruncK8sException(
                f"Failed to create pod '{session}.{podname}': {e}"
            ) from e

    def _get_connection_server_cluster_ip(self, session) -> str:
        """Gets the ClusterIP of the connection server service."""
        try:
            service = self._core_v1_api.read_namespaced_service(
                name=self.connection_server_name, namespace=session
            )
            return service.spec.cluster_ip
        except self._api_error_v1_api as e:
            self.log.error(f"Failed to get connection server service IP: {e}")
            return None

    def _extract_port_from_cmd(self, boot_request) -> int | None:
        """
        Parses the boot request's command arguments to find a port.
        It must cover Gunicorn (hardcoded and env var) and drunc-controller.
        """
        # Check all command parts for a port argument
        for e_and_a in boot_request.process_description.executable_and_arguments:
            all_args = [e_and_a.exec] + list(e_and_a.args)
            arg_str = " ".join(all_args)

            # Check for gunicorn bind syntax (for local-connection-server)
            if "gunicorn" in arg_str:
                match_hardcoded = re.search(r"(-b|--bind)[\s=]+[\w\.]+:(\d+)", arg_str)

                if match_hardcoded:
                    port = int(match_hardcoded.group(2))
                    if port != 0:
                        self.log.info(
                            f"Extracted hardcoded gunicorn port {port} from command."
                        )
                        return port

                # Match environment variable port: e.g., --bind=0.0.0.0:${CONNECTION_PORT}
                match_var = re.search(r"(-b|--bind)[\s=]+[\w\.]+:\$\{(\w+)\}", arg_str)

                if match_var:
                    var_name = match_var.group(2)
                    # Look up the value in the environment variables
                    port_val = boot_request.process_description.env.get(var_name)

                    if port_val is not None:
                        try:
                            port = int(port_val)
                            if port != 0:
                                self.log.info(
                                    f"Extracted gunicorn port {port} from environment variable '{var_name}'."
                                )
                                return port
                        except ValueError:
                            self.log.error(
                                f"Environment variable '{var_name}' ('{port_val}') is not an integer port."
                            )
                    else:
                        self.log.warning(
                            f"Extracted port variable '{var_name}' but it was not found in environment map."
                        )

            # Check for drunc-controller --port syntax (unchanged)
            if "controller" in arg_str:
                match = re.search(r"--port[\s=]+(\d+)", arg_str)
                if match:
                    port = int(match.group(1))
                    if port != 0:
                        self.log.info(
                            f"Extracted drunc-controller port {port} from command."
                        )
                        return port

            # Check for drunc-controller -c grpc://... syntax (unchanged)
            if "controller" in arg_str:
                match = re.search(r"-c\s+[\"\']?grpc:\/\/[^:]+:(\d+)[\"\']?", arg_str)
                if match:
                    port = int(match.group(1))
                    if port != 0:
                        self.log.info(
                            f"Extracted drunc-controller gRPC port {port} from command."
                        )
                        return port
                    else:
                        self.log.warning(
                            "Controller gRPC port is 0, cannot create NodePort."
                        )

        return None

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

    def _run_pre_boot_checks(
        self, session: str, podname: str, boot_request: BootRequest
    ) -> None:
        """Performs initial validation."""
        if not validate_k8s_session_name(session):
            raise DruncK8sNamespaceException(
                f'Invalid session/namespace name "{session}". Must match RFC1123 label: '
                "lowercase alphanumeric or '-', start/end with alphanumeric, max 63 chars."
            )

    def _wait_for_pod_api_ready(
        self, podname: str, session: str, timeout: float
    ) -> str:
        """
        [HELPER] Blocking wait for a pod to be 'Running' and 'Ready'
        in the K8s API.
        Returns the node_name on success.
        Raises DruncK8sException on timeout.
        """
        self.log.info(
            f"Stage 1: Waiting for '{podname}' pod to be Running and Ready..."
        )
        start_time = time()

        while time() - start_time < timeout:
            try:
                pod_status = self._core_v1_api.read_namespaced_pod_status(
                    podname, session
                )
                if pod_status.status.phase == "Running":
                    is_ready = False
                    if pod_status.status.conditions:
                        for condition in pod_status.status.conditions:
                            if condition.type == "Ready" and condition.status == "True":
                                is_ready = True
                                break

                    if is_ready:
                        node_name = pod_status.spec.node_name
                        self.log.info(
                            f"Stage 1: Pod '{podname}' is API Ready on node {node_name}."
                        )
                        return node_name  # Success!

            except self._api_error_v1_api as e:
                if e.status == 404:
                    # Pod not created yet, this is expected, continue loop
                    pass
                else:
                    # Re-raise other K8s API errors
                    raise e

            sleep(self.pod_status_check_sleep)

        # If we exit the loop, it's a timeout
        raise DruncK8sException(
            f"'{podname}' pod did not become API Ready in {timeout} seconds."
        )

    def _wait_for_nodeport_http_ready(self, url: str, timeout: float) -> None:
        """
        [HELPER] Blocking wait for a NodePort URL to be reachable via HTTP.
        Raises DruncK8sException on timeout.
        """
        self.log.info(f"Stage 2: Waiting for NodePort {url} to be reachable...")
        start_time = time()

        while time() - start_time < timeout:
            try:
                urllib.request.urlopen(url, timeout=1)
                self.log.info(f"Stage 2: NodePort {url} is now active.")
                return  # Success!
            except (
                urllib.error.URLError,
                ConnectionRefusedError,
                TimeoutError,
                OSError,
            ) as e:
                self.log.debug(f"NodePort not ready yet ({e}), retrying...")
                sleep(self.pod_status_check_sleep)

        raise DruncK8sException(
            f"NodePort {url} did not become reachable in {timeout} seconds."
        )

    def _wait_for_nodeport_tcp_ready(
        self, node_name: str, port: int, timeout: float
    ) -> None:
        """
        [HELPER] Blocking wait for a NodePort to be reachable via TCP socket.
        Raises DruncK8sException on timeout.
        """
        self.log.info(
            f"Stage 2: Waiting for NodePort {node_name}:{port} to be reachable..."
        )
        start_time = time()

        while time() - start_time < timeout:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(self.socket_retry_timeout)
                    result = sock.connect_ex((node_name, port))

                    if result == 0:
                        self.log.info(
                            f"Stage 2: NodePort {node_name}:{port} is active (TCP connect success)."
                        )
                        return
                    else:
                        self.log.debug(
                            f"NodePort {node_name}:{port} not ready yet (socket error {result}), retrying..."
                        )

            except socket.gaierror as e:
                self.log.warning(
                    f"Failed to resolve hostname '{node_name}': {e}. Retrying..."
                )
            except Exception as e:
                self.log.debug(
                    f"NodePort not ready yet (Socket error: {e}), retrying..."
                )

            sleep(self.pod_status_check_sleep)

        raise DruncK8sException(
            f"NodePort {node_name}:{port} did not become reachable in {timeout} seconds."
        )

    def _wait_for_lcs_readiness(self, podname: str, session: str) -> None:
        """Blocking two-stage wait for the Local Connection Server (NodePort) to be fully ready."""
        self.log.info(f"Waiting for LCS '{podname}' to be fully ready...")
        start_time = time()
        total_timeout = self.pod_ready_timeout

        # --- STAGE 1: Wait for Pod to be Running/Ready in K8s API ---
        node_name = self._wait_for_pod_api_ready(podname, session, total_timeout)

        # --- STAGE 2: Wait for NodePort to be externally reachable (using HTTP urllib) ---
        url = f"http://{node_name}:{self.connection_server_node_port}"

        # Calculate remaining time for stage 2, preserving original logic
        elapsed_stage1 = time() - start_time
        remaining_time = total_timeout - elapsed_stage1

        if remaining_time <= 0:
            raise DruncK8sException(
                f"NodePort {url} check failed: No time left after API readiness."
            )

        self._wait_for_nodeport_http_ready(url, remaining_time)

        self.local_connection_server_is_booted = True
        self.log.info(f"Connection server '{podname}' is fully ready.")

    def _wait_for_controller_readiness(
        self, podname: str, session: str, boot_request: BootRequest
    ) -> None:
        """Blocking two-stage wait for Drunc Controller (NodePort) to be fully ready."""
        self.log.info(
            f"Waiting for controller '{podname}' (NodePort) to become ready..."
        )

        controller_port = self._extract_port_from_cmd(boot_request)
        if not controller_port or controller_port == 0:
            raise DruncK8sException(
                f"Cannot wait for '{podname}', port is 0 or missing."
            )

        # --- STAGE 1: Wait for Pod to be Running/Ready in K8s API ---
        node_name = self._wait_for_pod_api_ready(
            podname, session, self.pod_ready_timeout
        )

        # --- STAGE 2: Wait for NodePort to be externally reachable (using TCP socket) ---
        self._wait_for_nodeport_tcp_ready(
            node_name, controller_port, self.grpc_startup_timeout
        )

        self.log.info(f"Drunc controller '{podname}' is fully ready.")

    def __boot(self, boot_request: BootRequest, uuid: str) -> ProcessInstance:
        """
        Internal boot method. Handles pre-checks, pod creation, and blocking wait for critical services.
        """
        session = boot_request.process_description.metadata.session
        podname = boot_request.process_description.metadata.name

        # Pre-checks (Session validation, NodePort collision)
        self._run_pre_boot_checks(session, podname, boot_request)

        # Resource Creation (Namespace, Pod, Labels)
        self._create_namespace(session)
        self.boot_request[uuid] = BootRequest()
        self.boot_request[uuid].CopyFrom(boot_request)

        self._create_pod(podname, session, boot_request)
        self._add_label(podname, "pod", "uuid", uuid, session=session)
        self.log.info(f'"{session}.{podname}":{uuid} boot request sent.')

        # Special handling and blocking wait for critical processes
        if podname == self.connection_server_name:
            self._wait_for_lcs_readiness(podname, session)
        elif "root-controller" in podname:
            self._wait_for_controller_readiness(podname, session, boot_request)

        # Post-Process
        pd, pr, pu = (
            ProcessDescription(),
            ProcessRestriction(),
            ProcessUUID(uuid=uuid),
        )
        pd.CopyFrom(boot_request.process_description)
        pr.CopyFrom(boot_request.process_restriction)

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
        if not uuids:
            raise DruncK8sPodException("No processes found matching the query.")

        # Create copies of boot requests for each process
        br_by_uuid = {}
        for u in uuids:
            br = BootRequest()
            br.CopyFrom(self.boot_request[u])
            br_by_uuid[u] = br

        ret = []
        for u in uuids:
            try:
                if u in self.boot_request:
                    pod_name = self.boot_request[u].process_description.metadata.name
                    session = self.boot_request[u].process_description.metadata.session

                    self.log.info(f"Restarting {pod_name} in session {session}")

                    # Kill the existing process
                    kill_query = ProcessQuery(uuids=[ProcessUUID(uuid=u)])
                    self._kill_impl(kill_query)

                    # Handle case where pod completes but isn't deleted (race condition fix)
                    try:
                        pod_status = self._core_v1_api.read_namespaced_pod_status(
                            pod_name, session
                        )
                        if pod_status.status.phase in ["Succeeded", "Failed"]:
                            self.log.info(
                                f"Pod {pod_name} is in terminal state {pod_status.status.phase}, deleting it"
                            )
                            self._core_v1_api.delete_namespaced_pod(
                                name=pod_name, namespace=session
                            )
                            sleep(2)  # Wait for deletion to complete
                    except self._api_error_v1_api as e:
                        if e.status != 404:  # 404 means pod is already deleted
                            self.log.warning(
                                f"Error checking pod status after kill: {e}"
                            )

                # Boot the new process
                pi = self.__boot(br_by_uuid[u], u)
                ret.append(pi)

            except Exception as e:
                self.log.error(f"Restart failed for UUID {u}: {e!s}")

                # Create a dead process instance for failed restarts
                pd = ProcessDescription()
                pr = ProcessRestriction()
                try:
                    pd.CopyFrom(br_by_uuid[u].process_description)
                    pr.CopyFrom(br_by_uuid[u].process_restriction)
                except Exception:
                    pass

                ret.append(
                    ProcessInstance(
                        process_description=pd,
                        process_restriction=pr,
                        status_code=ProcessInstance.StatusCode.DEAD,
                        return_code=None,
                        uuid=ProcessUUID(uuid=u),
                    )
                )

        return ProcessInstanceList(values=ret)

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

        apps = []
        for uuid_str in uuids_to_kill:
            if uuid_str not in self.boot_request:
                continue

            apps.append(uuid_str)

        def kill_and_wait(uuids, grace_period=None) -> None:
            if not uuids:
                return
            action = (
                "Forcing termination of"
                if grace_period == 0
                else "Gracefully terminating"
            )
            self.log.info(f"{action} {len(uuids)} pod(s)...")

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

            wait_timeout = (
                self.kill_timeout if grace_period is None else grace_period + 5
            )
            if not self.termination_complete_event.wait(timeout=wait_timeout):
                self.log.warning(f"Timeout. Remaining: {self.uuids_pending_deletion}")

            self.uuids_pending_deletion.clear()

        kill_and_wait(apps)

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
