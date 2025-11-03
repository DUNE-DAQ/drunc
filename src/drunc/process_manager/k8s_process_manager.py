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
            if e.status != 409:
                self.log.error(f"Failed to create NodePort service for {podname}: {e}")

    def _create_pod(self, podname, session, boot_request: BootRequest) -> None:
        """Constructs and creates a Kubernetes Pod manifest."""
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

        # Create container with conditional lifecycle hook
        container_kwargs = {
            "name": podname,
            "image": pod_image,
            "command": ["/bin/sh", "-c"],
            "args": [main_command_str],
            "env": [
                client.V1EnvVar(name=k, value=v)
                for k, v in boot_request.process_description.env.items()
            ],
            "ports": [],
            "volume_mounts": [
                client.V1VolumeMount(name="nfs", mount_path="/nfs"),
                client.V1VolumeMount(name="cvmfs", mount_path="/cvmfs"),
            ],
            "working_dir": boot_request.process_description.process_execution_directory,
            "security_context": client.V1SecurityContext(
                run_as_user=os.getuid(), run_as_group=os.getgid()
            ),
        }

        # Only add lifecycle hook for C++ applications
        if lifecycle_hook is not None:
            container_kwargs["lifecycle"] = lifecycle_hook

        main_container = client.V1Container(**container_kwargs)

        all_containers = [main_container]

        node_selector = {}
        if boot_request.process_restriction.allowed_hosts:
            target_host = boot_request.process_restriction.allowed_hosts[0]
            # Resolve localhost to actual hostname for Kubernetes node selection
            if target_host == "localhost":
                target_host = resolve_localhost_to_hostname(target_host)
                self.log.info(
                    f"Resolved localhost to '{target_host}' for node selection"
                )

            # Verify the target host is available in the cluster before scheduling
            self._verify_host_in_cluster(target_host)

            node_selector = {"kubernetes.io/hostname": target_host}
            self.log.info(
                f"Pod '{podname}' will be scheduled on node '{target_host}' (from boot request)"
            )

        host_aliases = []
        if (
            podname != self.connection_server_name
            and self.local_connection_server_is_booted
        ):
            # Wait for service to get ClusterIP
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

        pod_manifest = client.V1Pod(
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
                containers=all_containers,
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

        try:
            start_time = time()
            pod_uid = None

            while True:
                try:
                    created_pod = self._core_v1_api.create_namespaced_pod(
                        session, pod_manifest
                    )
                    self.log.info(f'Creating pod "{session}.{podname}"')
                    pod_uid = created_pod.metadata.uid
                    break

                # this covers restart where we need to wait for cleanup
                except self._api_error_v1_api as e:
                    is_409_conflict = e.status == 409

                    if (
                        is_409_conflict
                        and time() - start_time < self.restart_cleanup_time
                    ):
                        sleep(self.restart_cleanup_polling)
                        continue
                    raise e

            if podname == self.connection_server_name:
                self._create_nodeport_service(podname, session, pod_uid)
            elif "root-controller" in podname:
                self.log.info(
                    f"'{podname}' is the root controller, creating isolated NodePort service."
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

        except self._api_error_v1_api as e:
            error_message = f'Couldn\'t create resources for pod "{session}.{podname}". Reason: {e.reason}. Kubernetes API Error: ({e.status})'

            if e.status == 409 and time() - start_time >= self.restart_cleanup_time:
                error_message = (
                    f"Timeout (>{self.restart_cleanup_time}s) waiting for old pod object "
                    f'"{session}/{podname}" to be fully deleted. Could not restart pod.'
                )

            self.log.error(error_message)
            raise DruncK8sException(error_message) from e

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
        """
        # Check all command parts for a port argument
        for e_and_a in boot_request.process_description.executable_and_arguments:
            all_args = [e_and_a.exec] + list(e_and_a.args)
            arg_str = " ".join(all_args)

            # 1. Check for gunicorn bind syntax (for local-connection-server)
            # e.g., gunicorn --bind=0.0.0.0:30005
            if "gunicorn" in arg_str:
                match = re.search(r"-b\s+[\w\.]+:(\d+)", arg_str)
                if not match:
                    match = re.search(r"--bind[\s=]+[\w\.]+:(\d+)", arg_str)
                if match:
                    port = int(match.group(1))
                    if port != 0:
                        self.log.info(f"Extracted gunicorn port {port} from command.")
                        return port

            # 2. Check for drunc-controller --port syntax
            # e.g., drunc-controller --port 12345
            if "controller" in arg_str:
                # Matches --port 12345 or --port=12345
                match = re.search(r"--port[\s=]+(\d+)", arg_str)
                if match:
                    port = int(match.group(1))
                    if port != 0:
                        self.log.info(
                            f"Extracted drunc-controller port {port} from command."
                        )
                        return port

            # 3. NEW: Check for drunc-controller -c grpc://... syntax
            # e.g., drunc-controller -c grpc://localhost:12345 or -c grpc://localhost:0
            if "controller" in arg_str:
                # Matches -c grpc://...:12345 or -c "grpc://...:12345"
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

        # If no non-zero port is found, return None
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

    def __boot(self, boot_request: BootRequest, uuid: str) -> ProcessInstance:
        """
        Internal boot method. Handles pod creation and special logic for the connection server.
        - For the connection server: Wait for it to be ready and check the NodePort service
        - For all other pods: Boot is NON-BLOCKING.
        """
        print(boot_request)
        session = boot_request.process_description.metadata.session
        podname = boot_request.process_description.metadata.name

        if not validate_k8s_session_name(session):
            raise DruncK8sNamespaceException(
                f'Invalid session/namespace name "{session}". Must match RFC1123 label: '
                "lowercase alphanumeric or '-', start/end with alphanumeric, max 63 chars."
            )

        if boot_request.process_restriction.allowed_hosts:
            hostname = boot_request.process_restriction.allowed_hosts[0]
            boot_request.process_description.metadata.hostname = hostname

        if uuid in self.boot_request:
            raise DruncK8sPodException(f'"{session}.{podname}":{uuid} already exists!')

        # Extract ports for LCS
        if podname == self.connection_server_name:
            self.log.info(f"Waiting for '{podname}' to become ready...")

            port = None
            env_vars = boot_request.process_description.env

            if "CONNECTION_PORT" in env_vars:
                port_str = env_vars["CONNECTION_PORT"]
                try:
                    port = int(port_str)
                    self.log.info(
                        f"Using port {port} from 'CONNECTION_PORT' environment variable."
                    )
                except (ValueError, TypeError):
                    raise DruncK8sException(
                        f"The provided CONNECTION_PORT '{port_str}' is not a valid integer."
                    )

            if port is None:
                self.log.info(
                    "CONNECTION_PORT not found in env, falling back to parsing gunicorn command."
                )
                port = self._extract_port_from_cmd(boot_request)

            if port:
                self.connection_server_port = port
                self.connection_server_node_port = port
            else:
                raise DruncK8sException(
                    "Could not determine connection server port from 'CONNECTION_PORT' env var or gunicorn command."
                )

            # Check for NodePort collision
            api = self._core_v1_api
            all_services = api.list_service_for_all_namespaces()
            for svc in all_services.items:
                if not svc.spec.type == "NodePort":
                    continue
                for p in svc.spec.ports:
                    if p.node_port == self.connection_server_node_port and (
                        svc.metadata.namespace != session
                        or svc.metadata.name != podname
                    ):
                        raise DruncK8sException(
                            f"NodePort {self.connection_server_node_port} is already in use by service "
                            f"{svc.metadata.name} in namespace {svc.metadata.namespace}. "
                            "Cannot start another local connection server with the same port."
                        )

        self._create_namespace(session)

        self.boot_request[uuid] = BootRequest()
        self.boot_request[uuid].CopyFrom(boot_request)

        self._create_pod(podname, session, boot_request)
        self._add_label(podname, "pod", "uuid", uuid, session=session)
        self.log.info(f'"{session}.{podname}":{uuid} boot request sent.')

        # Special handling only for the connection server
        if podname == self.connection_server_name:
            node_name = None
            pod_ready = False
            start_time = time()

            # --- STAGE 1: Wait for Pod to be Running/Ready in K8s API ---
            self.log.info(
                f"Stage 1: Waiting for '{podname}' pod to be Running and Ready..."
            )
            while not pod_ready and (time() - start_time < self.pod_ready_timeout):
                try:
                    pod_status = self._core_v1_api.read_namespaced_pod_status(
                        podname, session
                    )
                    if (
                        pod_status.status.phase == "Running"
                        and pod_status.status.pod_ip
                    ):
                        # Check readiness condition
                        is_ready = False
                        if pod_status.status.conditions:
                            for condition in pod_status.status.conditions:
                                if (
                                    condition.type == "Ready"
                                    and condition.status == "True"
                                ):
                                    is_ready = True
                                    break

                        if is_ready:
                            self.log.info(
                                f"Stage 1: Pod '{podname}' is API Ready with IP {pod_status.status.pod_ip}."
                            )
                            node_name = pod_status.spec.node_name
                            pod_ready = True  # Exit this loop and go to Stage 2

                except self._api_error_v1_api as e:
                    if e.status == 404:
                        pass  # Pod not yet created/visible, keep polling
                    else:
                        raise e
                sleep(self.pod_status_check_sleep)

            if not pod_ready:
                raise DruncK8sException(
                    f"'{podname}' pod did not become API Ready in {self.pod_ready_timeout} seconds."
                )

            # --- STAGE 2: Wait for NodePort to be externally reachable ---
            self.log.info(
                f"Stage 2: Waiting for NodePort {node_name}:{self.connection_server_node_port} to be reachable..."
            )
            nodeport_ready = False
            url = f"http://{node_name}:{self.connection_server_node_port}"

            # Use the *remaining* time for this check
            remaining_time = self.pod_ready_timeout - (time() - start_time)
            nodeport_start_time = time()

            while not nodeport_ready and (
                time() - nodeport_start_time < remaining_time
            ):
                try:
                    # We don't care about the response, just that it doesn't error
                    # Timeout set to 1s for a quick check
                    urllib.request.urlopen(url, timeout=1)
                    nodeport_ready = True
                    self.log.info(f"Stage 2: NodePort {url} is now active.")
                except (
                    urllib.error.URLError,
                    ConnectionRefusedError,
                    TimeoutError,
                    OSError,
                ) as e:
                    # Keep polling until timeout
                    self.log.debug(f"NodePort not ready yet ({e}), retrying...")
                    sleep(self.pod_status_check_sleep)

            if not nodeport_ready:
                raise DruncK8sException(
                    f"NodePort {url} did not become reachable in {self.pod_ready_timeout} seconds."
                )

            # --- All clear ---
            self.local_connection_server_is_booted = True

            self.log.info(f"Connection server '{podname}' is fully ready.")
            self.log.info(
                f" -> For internal cluster access: 'http://localhost:{self.connection_server_port}'"
            )
            self.log.info(
                f" -> For external access, use NodePort {self.connection_server_node_port} on any cluster node IP (e.g., http://{node_name}:{self.connection_server_node_port})"
            )

        elif "root-controller" in podname:
            self.log.info(f"Waiting for '{podname}' (HostPort) to become ready...")
            node_name = None
            pod_ready = False
            controller_port = self._extract_port_from_cmd(boot_request)
            
            if not controller_port or controller_port == 0:
                 raise DruncK8sException(f"Cannot wait for '{podname}', port is 0 or missing.")

            # --- STAGE 1: Wait for Pod to be Running/Ready in K8s API ---
            self.log.info(f"Stage 1: Waiting for '{podname}' pod to be Running and Ready...")
            start_time = time()
            api_ready_timeout = self.pod_ready_timeout # Use standard pod timeout
            
            while not pod_ready and (time() - start_time < api_ready_timeout):
                try:
                    pod_status = self._core_v1_api.read_namespaced_pod_status(
                        podname, session
                    )
                    if (
                        pod_status.status.phase == "Running"
                        and pod_status.status.pod_ip
                    ):
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
                            pod_ready = True # Exit this loop and go to Stage 2
                
                except self._api_error_v1_api as e:
                    if e.status == 404: pass # Pod not yet created/visible
                    else: raise e
                sleep(self.pod_status_check_sleep)
            
            if not pod_ready:
                raise DruncK8sException(
                    f"'{podname}' pod did not become API Ready in {api_ready_timeout} seconds."
                )

            # --- STAGE 2: Wait for HostPort to be externally reachable (using TCP socket) ---
            self.log.info(f"Stage 2: Waiting for HostPort {node_name}:{controller_port} to be reachable...")
            hostport_ready = False
            
            grpc_startup_timeout = 120 
            hostport_start_time = time()
            
            while not hostport_ready and (time() - hostport_start_time < grpc_startup_timeout):
                # We will try to open a simple TCP socket instead of using HTTP
                sock = None # Initialize sock to None
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(1.0) # 1 second timeout
                    # Try to connect
                    result = sock.connect_ex((node_name, controller_port))
                    
                    if result == 0:
                        # 0 means the connection was successful
                        hostport_ready = True
                        self.log.info(f"Stage 2: HostPort {node_name}:{controller_port} is active (TCP connect success).")
                    else:
                        # Connection failed (e.g., connection refused, no route)
                        self.log.debug(f"HostPort {node_name}:{controller_port} not ready yet (socket error {result}), retrying...")
                        sleep(self.pod_status_check_sleep) # Wait before retrying
                        
                except socket.gaierror as e:
                     # Handle DNS name resolution error (e.g., node_name not found)
                     self.log.warning(f"Failed to resolve hostname '{node_name}': {e}. Retrying...")
                     sleep(self.pod_status_check_sleep)
                except Exception as e:
                    # Catch any other socket errors
                    self.log.debug(f"HostPort not ready yet (Socket error: {e}), retrying...")
                    sleep(self.pod_status_check_sleep)
                finally:
                    if sock:
                        sock.close() # Always close the socket
            
            if not hostport_ready:
                raise DruncK8sException(
                    f"HostPort {node_name}:{controller_port} did not become reachable in {grpc_startup_timeout} seconds."
                )
            
            self.log.info(f"Controller '{podname}' is fully ready.")

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
