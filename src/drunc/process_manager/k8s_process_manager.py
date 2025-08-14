# Standard Library Imports
import getpass
import os
import re
import signal
import subprocess
import threading
import uuid
from time import sleep, time

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

# Local Application Imports
from drunc.exceptions import DruncCommandException, DruncException
from drunc.process_manager.process_manager import ProcessManager
from drunc.utils.utils import get_logger


class K8sPodWatcherThread(threading.Thread):
    def __init__(self, pm):
        threading.Thread.__init__(self)
        self.pm = pm
        self.daemon = True

    def run(self):
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
                    uuid = metadata.labels.get(f"uuid.{self.pm.drunc_label}")
                    session = metadata.namespace

                    if not uuid:
                        continue

                    if event["type"] in ["MODIFIED", "DELETED"] and phase in ["Succeeded", "Failed"]:
                        exit_code = -1
                        reason = "Unknown"
                        if status.container_statuses and status.container_statuses[0].state.terminated:
                            terminated_state = status.container_statuses[0].state.terminated
                            exit_code = terminated_state.exit_code
                            reason = terminated_state.reason
                        self.pm.notify_termination(uuid, exit_code, reason, session)

            except Exception as e:
                self.pm.log.error(f"K8s watcher thread error: {e}. Restarting watch.")
                sleep(5)


class K8sProcessManager(ProcessManager):
    def __init__(self, configuration, **kwargs):
        """
        Manages processes as Kubernetes Pods.

        This ProcessManager interfaces with the Kubernetes API to start, stop, and monitor
        applications running in Pods. It includes special handling for a local connectivity
        service, which involves:
        1.  Automating a `kubectl port-forward` in a background shell process for the orchestrator.
        2.  Injecting a proxy sidecar container into other pods for transparent service discovery.
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

        self.drunc_label = "drunc.daq"
        self.watchers = []
        self._start_watcher()
        self.sessions_pending_deletion = set()
        self.port_forwards = {}
        self.local_connection_server_is_booted = False

        # Safely get settings from the configuration object
        settings = {}
        if hasattr(self.configuration.data, 'settings') and self.configuration.data.settings is not None:
            settings = self.configuration.data.settings

        self.connection_server_name = settings.get("connection_server_name", "local-connection-server")
        self.connection_server_port = settings.get("connection_server_port", 5000)
        self.sidecar_image = settings.get("sidecar_image", "alpine/socat")
        self.pod_ready_timeout = settings.get("pod_ready_timeout", 60)
        self.port_forward_timeout = settings.get("port_forward_timeout", 15)
        self.kill_timeout = settings.get("kill_timeout", 10)

        self.log.debug(f'Using kill_timeout of {self.kill_timeout} seconds.')

        namespaces = self._core_v1_api.list_namespace(
            label_selector=f"creator.{self.drunc_label}={self.__class__.__name__}"
        )
        namespace_names = [ns.metadata.name for ns in namespaces.items]
        namespace_list_str = "\n - ".join(namespace_names)

        if namespace_list_str:
            self.log.info(f"Active namespaces created by drunc:\n - {namespace_list_str}")
        else:
            self.log.info("No active namespace created by drunc")

    def _start_watcher(self):
        """Starts the background thread that watches for Pod status changes."""
        self.log.debug("Starting K8s pod watcher thread")
        t = K8sPodWatcherThread(pm=self)
        t.start()
        self.watchers.append(t)

    def notify_termination(self, uuid, exit_code, reason, session):
        """Callback for when a pod terminates."""
        if uuid in self.boot_request:
            meta = self.boot_request[uuid].process_description.metadata
            end_str = f"Pod '{meta.name}' (session: '{session}', user: '{meta.user}', uuid: {uuid}) terminated with exit code {exit_code}. Reason: {reason}"
            self.log.info(end_str)
            self.broadcast(end_str, BroadcastType.SUBPROCESS_STATUS_UPDATE)
        else:
            self.log.debug(f"Received termination for already-removed UUID {uuid}, checking session '{session}' for cleanup.")

        self._kill_if_empty_session(session)

    def is_alive(self, podname, session):
        """Checks if a pod is currently in the 'Running' phase."""
        try:
            pod_status = self._core_v1_api.read_namespaced_pod_status(podname, session)
            return pod_status.status.phase == "Running"
        except self._api_error_v1_api as e:
            if e.status == 404: return False
            self.log.error(f"Error checking status for pod {session}.{podname}: {e}")
            return False

    def _add_label(self, obj_name, obj_type, key, label, session=None):
        """Adds a label to a Kubernetes object (Pod or Namespace)."""
        body = {"metadata": {"labels": {f"{key}.{self.drunc_label}": label}}}

        if obj_type == "pod":
            if not session: raise DruncException("Session (namespace) must be provided to label a pod.")
            try:
                self._core_v1_api.patch_namespaced_pod(name=obj_name, namespace=session, body=body)
                self.log.info(f'Added label "{key}.{self.drunc_label}:{label}" to pod "{session}.{obj_name}"')
            except self._api_error_v1_api as e:
                self.log.error(f"Failed to apply label to pod {session}/{obj_name}: {e}")
        elif obj_type == "namespace":
            try:
                self._core_v1_api.patch_namespace(name=obj_name, body=body)
                self.log.info(f'Added label "{key}.{self.drunc_label}:{label}" to namespace "{obj_name}"')
            except self._api_error_v1_api as e:
                self.log.error(f"Failed to apply label to namespace {obj_name}: {e}")
        else:
            raise DruncException(f"Cannot add label to object type: {obj_type}")

    def _add_creator_label(self, obj_name, obj_type):
        """Adds a 'creator' label to a Kubernetes object."""
        self._add_label(obj_name, obj_type, "creator", self.__class__.__name__)

    def _get_creator_label_selector(self):
        """Returns the label selector for objects created by this class."""
        return f"creator.{self.drunc_label}={self.__class__.__name__}"

    def _create_namespace(self, session):
        """Creates a Kubernetes namespace if it doesn't already exist."""
        if session in self.sessions_pending_deletion: self.sessions_pending_deletion.remove(session)
        try:
            self._core_v1_api.read_namespace(name=session)
        except self._api_error_v1_api as e:
            if e.status == 404:
                self.log.info(f'Creating "{session}" session')
                namespace_manifest = client.V1Namespace(
                    api_version="v1", kind="Namespace",
                    metadata=self._meta_v1_api(name=session, labels={"pod-security.kubernetes.io/enforce": "privileged"})
                )
                self._core_v1_api.create_namespace(body=namespace_manifest)
                self._add_creator_label(session, "namespace")
            else:
                raise e

    def _create_headless_service(self, podname, session, pod_uid):
        """Creates a headless service for a pod."""
        service_manifest = client.V1Service(
            api_version="v1", kind="Service",
            metadata=self._meta_v1_api(
                name=podname, namespace=session,
                labels={f"creator.{self.drunc_label}": self.__class__.__name__},
                owner_references=[client.V1OwnerReference(
                    api_version="v1", kind="Pod", name=podname, uid=pod_uid,
                    controller=True, block_owner_deletion=True
                )]
            ),
            spec=client.V1ServiceSpec(
                cluster_ip="None", selector={"app": podname},
                ports=[client.V1ServicePort(port=80, target_port=80)],
            ),
        )
        try:
            self._core_v1_api.create_namespaced_service(namespace=session, body=service_manifest)
            self.log.info(f'Created headless service "{session}.{podname}"')
        except self._api_error_v1_api as e:
            if e.status != 409: self.log.error(f"Failed to create headless service for {podname}: {e}")

    def _create_clusterip_service(self, podname, session, pod_uid):
        """Creates a standard ClusterIP service for the connection server."""
        service_manifest = client.V1Service(
            api_version="v1", kind="Service",
            metadata=self._meta_v1_api(
                name=podname, namespace=session,
                labels={f"creator.{self.drunc_label}": self.__class__.__name__},
                owner_references=[client.V1OwnerReference(
                    api_version="v1", kind="Pod", name=podname, uid=pod_uid,
                    controller=True, block_owner_deletion=True
                )]
            ),
            spec=client.V1ServiceSpec(
                selector={"app": podname},
                ports=[client.V1ServicePort(
                    protocol="TCP",
                    port=self.connection_server_port,
                    target_port=self.connection_server_port
                )]
            ),
        )
        try:
            self._core_v1_api.create_namespaced_service(namespace=session, body=service_manifest)
            self.log.info(f'Created ClusterIP service "{session}.{podname}" on port {self.connection_server_port}')
        except self._api_error_v1_api as e:
            if e.status != 409: self.log.error(f"Failed to create ClusterIP service for {podname}: {e}")

    def _create_pod(self, podname, session, boot_request: BootRequest):
        """Constructs and creates a Kubernetes Pod manifest."""
        pod_image = self.configuration.data.image
        exec_and_args_list = boot_request.process_description.executable_and_arguments
        main_command_str = "; ".join([" ".join([e_and_a.exec] + list(e_and_a.args)) for e_and_a in exec_and_args_list])
        init_containers = []
        if len(exec_and_args_list) > 1 and exec_and_args_list[0].exec == "source":
            env_script_path = exec_and_args_list[0].args[0]
            main_app_name = exec_and_args_list[1].exec
            init_command_str = f"until source {env_script_path} && command -v {main_app_name} >/dev/null 2>&1; do echo 'Waiting for env for {main_app_name}...'; sleep 1; done"
            init_containers.append(client.V1Container(
                name="init-environment", image=pod_image, command=["sh", "-c"], args=[init_command_str],
                volume_mounts=[
                    client.V1VolumeMount(name="nfs", mount_path="/nfs"),
                    client.V1VolumeMount(name="cvmfs", mount_path="/cvmfs"),
                ]
            ))

        main_container = client.V1Container(
            name=podname, image=pod_image, command=["sh", "-c"],
            args=[main_command_str],
            env=[client.V1EnvVar(name=k, value=v) for k, v in boot_request.process_description.env.items()],
            ports=[],
            volume_mounts=[
                client.V1VolumeMount(name="nfs", mount_path="/nfs"),
                client.V1VolumeMount(name="cvmfs", mount_path="/cvmfs"),
            ],
            working_dir=boot_request.process_description.process_execution_directory,
            security_context=client.V1SecurityContext(run_as_user=os.getuid(), run_as_group=os.getgid()),
        )

        all_containers = [main_container]

        # If the local connection server is active, inject a proxy sidecar into all other pods.
        if podname != self.connection_server_name and self.local_connection_server_is_booted:
            self.log.info(f"Adding proxy sidecar to pod '{podname}'")
            sidecar_container = client.V1Container(
                name="proxy-sidecar",
                image=self.sidecar_image,
                args=[
                    f"TCP-LISTEN:{self.connection_server_port},fork,reuseaddr",
                    f"TCP:{self.connection_server_name}.{session}:{self.connection_server_port}"
                ]
            )
            all_containers.append(sidecar_container)

        pod_manifest = client.V1Pod(
            api_version="v1", kind="Pod",
            metadata=self._meta_v1_api(
                name=podname, namespace=session,
                labels={"app": podname, f"creator.{self.drunc_label}": self.__class__.__name__}
            ),
            spec=self._pod_spec_v1_api(
                init_containers=init_containers,
                restart_policy="Never",
                containers=all_containers,
                volumes=[
                    client.V1Volume(name="nfs", host_path=client.V1HostPathVolumeSource(path="/nfs")),
                    client.V1Volume(name="cvmfs", host_path=client.V1HostPathVolumeSource(path="/cvmfs")),
                ],
            ),
        )
        try:
            created_pod = self._core_v1_api.create_namespaced_pod(session, pod_manifest)
            self.log.info(f'Creating pod "{session}.{podname}"')
            pod_uid = created_pod.metadata.uid

            if podname == self.connection_server_name:
                self._create_clusterip_service(podname, session, pod_uid)
            else:
                self._create_headless_service(podname, session, pod_uid)

        except self._api_error_v1_api as e:
            self.log.error(f'Couldn\'t create pod "{session}.{podname}": {e}')
            raise e

    def _get_process_uid(self, query: ProcessQuery, order_by: str = None):
        """
        Finds process UUIDs matching a query.
        
        If order_by is "leaf_first", it sorts the UUIDs so that child processes
        (which have a longer tree_id) come before their parents.
        """
        initial_match = set()
        for proc_uuid, boot_req in self.boot_request.items():
            meta = boot_req.process_description.metadata
            query_is_empty = not any([query.uuids, query.names, query.session, query.user])

            if query_is_empty or \
               any(uid.uuid == proc_uuid for uid in query.uuids) or \
               (query.session and query.session == meta.session) or \
               (query.user and query.user == meta.user) or \
               any(re.search(name_reg, meta.name) for name_reg in query.names):
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
            return LogLines(uuid=ProcessUUID(uuid=uuid), lines=logs.split('\n'))
        except self._api_error_v1_api as e:
            return LogLines(uuid=ProcessUUID(uuid=uuid), lines=[f"Could not retrieve logs: {e.reason}"])

    def _boot_impl(self, boot_request: BootRequest) -> ProcessInstanceList:
        """Handles the 'boot' command from the gRPC interface."""
        self.log.debug(f"{self.name} running boot command")
        this_uuid = str(uuid.uuid4())
        process = self.__boot(boot_request, this_uuid)
        return ProcessInstanceList(values=[process])

    def __boot(self, boot_request: BootRequest, uuid: str) -> ProcessInstance:
        """
        Internal boot method. Handles pod creation and special logic for the connection server.
        - For the connection server: Boot is BLOCKING, and a port-forward is automated.
        - For all other pods: Boot is NON-BLOCKING.
        """
        session = boot_request.process_description.metadata.session
        podname = boot_request.process_description.metadata.name

        session_re = re.compile(r'^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$')
        if not session_re.match(session):
            raise DruncCommandException(f'Invalid session/namespace name "{session}". Must match RFC1123 label: '
                "lowercase alphanumeric or '-', start/end with alphanumeric, max 63 chars.")

        if uuid in self.boot_request:
            raise DruncCommandException(f'"{session}.{podname}":{uuid} already exists!')

        self.boot_request[uuid] = BootRequest()
        self.boot_request[uuid].CopyFrom(boot_request)

        self._create_namespace(session)
        self._create_pod(podname, session, boot_request)
        self._add_label(podname, "pod", "uuid", uuid, session=session)
        self.log.info(f'"{session}.{podname}":{uuid} boot request sent.')

        # Special handling only for the connection server
        if podname == self.connection_server_name:
            # Step 1: Block and wait for the server pod to be fully ready
            self.log.info(f"Waiting for '{podname}' to become ready...")
            start_time = time()
            while time() - start_time < self.pod_ready_timeout:
                try:
                    pod_status = self._core_v1_api.read_namespaced_pod_status(podname, session)
                    if pod_status.status.phase == 'Running' and pod_status.status.pod_ip:
                        self.log.info(f"'{podname}' is ready with IP {pod_status.status.pod_ip}.")
                        self.local_connection_server_is_booted = True
                        break
                except self._api_error_v1_api as e:
                    if e.status == 404: pass # Not created yet, wait
                    else: raise e
                sleep(1)
            else: # Only runs if the while loop finishes without a break
                raise DruncException(f"'{podname}' did not become ready in {self.pod_ready_timeout} seconds.")

            # Step 2: Automate the port-forward for the orchestrator by running it in a shell
            kubeconfig_path = os.environ.get('KUBECONFIG')
            proxy_unset_script = "~np04daq/bin/web_proxy.sh -u"

            command_parts = []
            if kubeconfig_path:
                command_parts.append(f"export KUBECONFIG='{kubeconfig_path}'")
            else:
                self.log.warning("KUBECONFIG env var not set; assuming kubectl is configured.")
            
            command_parts.append(f"source {proxy_unset_script}")
            command_parts.append(f"kubectl port-forward -n {session} pod/{podname} {self.connection_server_port}:{self.connection_server_port}")
            command = "; ".join(command_parts)

            self.log.info("Starting port-forward for orchestrator. Executing shell command...")
            try:
                # preexec_fn=os.setsid creates a new process group, allowing us to kill the shell and all its children.
                proc = subprocess.Popen(
                    command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, preexec_fn=os.setsid
                )
                self.port_forwards[uuid] = proc
                self.log.info(f"Port-forward process for '{podname}' started with PID {proc.pid}. Waiting for tunnel...")

                # Wait for kubectl to confirm the tunnel is up to prevent a race condition
                output = {}
                def read_output(pipe, storage):
                    storage['line'] = pipe.readline()

                thread = threading.Thread(target=read_output, args=(proc.stdout, output))
                thread.start()
                thread.join(timeout=self.port_forward_timeout)

                if thread.is_alive():
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    raise DruncException(f"Port-forward for '{podname}' did not become ready in {self.port_forward_timeout} seconds.")

                line = output.get('line', '').strip()
                if "Forwarding from" in line:
                    self.log.info(f"Port-forward is active: {line}")
                else:
                    error_output = proc.stderr.read()
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    raise DruncException(f"Port-forward for '{podname}' failed. Stdout: '{line}'. Stderr: '{error_output}'")

            except Exception as e:
                self.log.error(f"Failed to start or validate port-forwarding for '{podname}': {e}")
                if uuid in self.port_forwards:
                    os.killpg(os.getpgid(self.port_forwards[uuid].pid), signal.SIGTERM)
                    del self.port_forwards[uuid]
                raise

        pd, pr, pu = ProcessDescription(), ProcessRestriction(), ProcessUUID(uuid=uuid)
        pd.CopyFrom(self.boot_request[uuid].process_description)
        pr.CopyFrom(self.boot_request[uuid].process_restriction)

        return ProcessInstance(
            process_description=pd, process_restriction=pr,
            status_code=ProcessInstance.StatusCode.RUNNING, uuid=pu,
        )

    def _ps_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        """Handles the 'ps' command."""
        queried_uuids = self._get_process_uid(query)
        if not queried_uuids: return ProcessInstanceList(values=[])
        all_pods = self._core_v1_api.list_pod_for_all_namespaces(
            label_selector=self._get_creator_label_selector()
        )
        uuid_to_pod = {p.metadata.labels.get(f"uuid.{self.drunc_label}"): p for p in all_pods.items}
        ret = []
        for proc_uuid in queried_uuids:
            if proc_uuid not in self.boot_request: continue
            pod = uuid_to_pod.get(proc_uuid)
            status_code = ProcessInstance.StatusCode.DEAD
            return_code = None
            if pod:
                if pod.status.phase == 'Running':
                    status_code = ProcessInstance.StatusCode.RUNNING
                elif pod.status.phase in ['Succeeded', 'Failed']:
                    if pod.status.container_statuses and pod.status.container_statuses[0].state.terminated:
                        return_code = pod.status.container_statuses[0].state.terminated.exit_code
            pd, pr, pu = ProcessDescription(), ProcessRestriction(), ProcessUUID(uuid=proc_uuid)
            pd.CopyFrom(self.boot_request[proc_uuid].process_description)
            pr.CopyFrom(self.boot_request[proc_uuid].process_restriction)
            ret.append(ProcessInstance(
                process_description=pd, process_restriction=pr,
                status_code=status_code, return_code=return_code, uuid=pu,
            ))
        return ProcessInstanceList(values=ret)

    def _restart_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        """Handles the 'restart' command."""
        uuids = self._get_process_uid(query)
        uuid = self._ensure_one_process(uuids, in_boot_request=True)

        if uuid not in self.boot_request:
            raise DruncCommandException(f"Cannot restart process with UUID {uuid}: Not found.")

        br_copy = BootRequest()
        br_copy.CopyFrom(self.boot_request[uuid])

        kill_query = ProcessQuery(uuids=[ProcessUUID(uuid=uuid)])
        self._kill_impl(kill_query)

        restarted_process = self.__boot(br_copy, uuid)

        return ProcessInstanceList(values=[restarted_process])

    def _kill_pod(self, podname, session, grace_period=None):
        """Deletes a specific pod from a namespace."""
        try:
            self._core_v1_api.delete_namespaced_pod(
                podname,
                session,
                grace_period_seconds=grace_period
            )
        except self._api_error_v1_api as e:
            if e.status != 404:
                raise e

    def _kill_if_empty_session(self, session):
        """Deletes a namespace if it contains no more pods managed by this process manager."""
        if session in self.sessions_pending_deletion: return
        try:
            pods = self._core_v1_api.list_namespaced_pod(
                session, label_selector=self._get_creator_label_selector()
            )
            if not pods.items:
                self.sessions_pending_deletion.add(session)
                self.log.info(f'Session "{session}" is empty, deleting namespace.')
                self._core_v1_api.delete_namespace(session)
        except self._api_error_v1_api as e:
            if e.status == 404: self.sessions_pending_deletion.add(session)
            else: self.log.warning(f"Failed to check/delete empty session {session}: {e}")

    def _kill_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        """Handles the 'kill' command."""
        ret = []
        uuids_to_kill = self._get_process_uid(query, order_by="leaf_first")

        for proc_uuid in uuids_to_kill:
            if proc_uuid not in self.boot_request: continue
            pd = self.boot_request[proc_uuid].process_description
            podname, session = pd.metadata.name, pd.metadata.session
            
            # If this pod has a port-forward process, terminate it.
            if proc_uuid in self.port_forwards:
                self.log.info(f"Terminating port-forward process for '{podname}'")
                proc = self.port_forwards.pop(proc_uuid)
                # Kill the entire process group started by shell=True
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                proc.wait()
                self.log.info(f"Port-forward process for '{podname}' terminated.")

            self.log.info(f'Killing pod "{session}/{podname}" (UUID {proc_uuid})')
            try:
                self._kill_pod(podname, session, grace_period=self.kill_timeout)
            except Exception as e:
                self.log.error(f"Failed to issue kill for pod {podname}: {e}")
                continue
            pd_copy, pr_copy, pu_copy = ProcessDescription(), ProcessRestriction(), ProcessUUID(uuid=proc_uuid)
            pd_copy.CopyFrom(self.boot_request[proc_uuid].process_description)
            pr_copy.CopyFrom(self.boot_request[proc_uuid].process_restriction)
            ret.append(
                ProcessInstance(
                    process_description=pd_copy, process_restriction=pr_copy,
                    status_code=ProcessInstance.StatusCode.DEAD, uuid=pu_copy,
                )
            )
            del self.boot_request[proc_uuid]
        return ProcessInstanceList(values=ret)

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

