import getpass
import os
import re
import socket
import threading
import uuid
from time import sleep

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
from kubernetes import client, config, watch

from drunc.exceptions import DruncCommandException, DruncException
from drunc.k8s_exceptions import DruncK8sNamespaceAlreadyExists
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
                    event_type = event["type"]
                    metadata = pod.metadata
                    status = pod.status
                    phase = status.phase
                    uuid = metadata.labels.get(f"uuid.{self.pm.drunc_label}")

                    if not uuid:
                        continue

                    if event_type in ["MODIFIED", "DELETED"] and phase in ["Succeeded", "Failed"]:
                        exit_code = -1
                        reason = "Unknown"
                        if status.container_statuses and status.container_statuses[0].state.terminated:
                            terminated_state = status.container_statuses[0].state.terminated
                            exit_code = terminated_state.exit_code
                            reason = terminated_state.reason
                        self.pm.notify_termination(uuid, exit_code, reason)

            except Exception as e:
                self.pm.log.error(f"K8s watcher thread error: {e}. Restarting watch.")
                sleep(5)


class K8sProcessManager(ProcessManager):
    def __init__(self, configuration, **kwargs):
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
        self.log.debug("Starting K8s pod watcher thread")
        t = K8sPodWatcherThread(pm=self)
        t.start()
        self.watchers.append(t)

    def notify_termination(self, uuid, exit_code, reason):
        if uuid not in self.boot_request:
            self.log.debug(f"Received termination for already-removed UUID {uuid}")
            return

        meta = self.boot_request[uuid].process_description.metadata
        end_str = f"Pod '{meta.name}' (session: '{meta.session}', user: '{meta.user}', uuid: {uuid}) terminated with exit code {exit_code}. Reason: {reason}"
        self.log.info(end_str)
        self.broadcast(end_str, BroadcastType.SUBPROCESS_STATUS_UPDATE)

    def is_alive(self, podname, session):
        try:
            pod_status = self._core_v1_api.read_namespaced_pod_status(podname, session)
            return pod_status.status.phase == "Running"
        except self._api_error_v1_api as e:
            if e.status == 404:
                return False
            else:
                self.log.error(f"Error checking status for pod {session}.{podname}: {e}")
                return False

    def _add_label(self, obj_name, obj_type, key, label, session=None):
        body = {"metadata": {"labels": {f"{key}.{self.drunc_label}": label}}}
        
        if obj_type == "pod":
            if not session:
                raise DruncException("Session (namespace) must be provided to label a pod.")
            try:
                self._core_v1_api.patch_namespaced_pod(name=obj_name, namespace=session, body=body)
            except self._api_error_v1_api as e:
                self.log.error(f"Failed to apply label to pod {session}/{obj_name}: {e}")

        elif obj_type == "namespace":
            try:
                self._core_v1_api.patch_namespace(name=obj_name, body=body)
            except self._api_error_v1_api as e:
                self.log.error(f"Failed to apply label to namespace {obj_name}: {e}")
        else:
            raise DruncException(f"Cannot add label to object type: {obj_type}")

    def _add_creator_label(self, obj_name, obj_type):
        self._add_label(obj_name, obj_type, "creator", self.__class__.__name__)

    def _get_creator_label_selector(self):
        return f"creator.{self.drunc_label}={self.__class__.__name__}"

    def _create_namespace(self, session):
        try:
            self._core_v1_api.read_namespace(name=session)
        except self._api_error_v1_api as e:
            if e.status == 404:
                self.log.info(f'Creating "{session}" session')
                namespace_manifest = client.V1Namespace(
                    api_version="v1",
                    kind="Namespace",
                    metadata=self._meta_v1_api(
                        name=session,
                        labels={"pod-security.kubernetes.io/enforce": "privileged"}
                    )
                )
                self._core_v1_api.create_namespace(body=namespace_manifest)
                self._add_creator_label(session, "namespace")
            else:
                raise e

    def _create_headless_service(self, podname, session, pod_uid):
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
            self._core_v1_api.create_namespaced_service(namespace=session, body=service_manifest)
            self.log.info(f'Created headless service "{session}.{podname}"')
        except self._api_error_v1_api as e:
            if e.status != 409: # Ignore if it already exists
                self.log.error(f"Failed to create headless service for {podname}: {e}")

    def _create_pod(self, podname, session, boot_request: BootRequest):
        pod_image = "ghcr.io/dune-daq/alma9:latest"
        
        original_exec_and_args = "; ".join([
            " ".join([e_and_a.exec] + list(e_and_a.args))
            for e_and_a in boot_request.process_description.executable_and_arguments
        ])

        wait_command = ""
        if boot_request.process_description.executable_and_arguments:
            first_e_and_a = boot_request.process_description.executable_and_arguments[0]
            file_to_wait_for = ""
            if first_e_and_a.exec == "source" and first_e_and_a.args:
                file_to_wait_for = first_e_and_a.args[0]
            else:
                file_to_wait_for = first_e_and_a.exec
            if file_to_wait_for:
                wait_command = f"while [ ! -f {file_to_wait_for} ]; do echo 'Waiting for {file_to_wait_for} to be available...'; sleep 0.5; done;"
        
        final_exec_and_args = f"{wait_command} {original_exec_and_args}"
        env_vars = [client.V1EnvVar(name=k, value=v) for k, v in boot_request.process_description.env.items()]

        pod_manifest = client.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=self._meta_v1_api(
                name=podname, 
                namespace=session,
                labels={"app": podname, f"creator.{self.drunc_label}": self.__class__.__name__}
            ),
            spec=self._pod_spec_v1_api(
                restart_policy="Never",
                containers=[
                    client.V1Container(
                        name=podname, image=pod_image, command=["sh", "-c"],
                        args=[final_exec_and_args], env=env_vars,
                        volume_mounts=[
                            client.V1VolumeMount(name="nfs", mount_path="/nfs"),
                            client.V1VolumeMount(name="cvmfs", mount_path="/cvmfs"),
                        ],
                        working_dir=boot_request.process_description.process_execution_directory,
                        security_context=client.V1SecurityContext(run_as_user=os.getuid(), run_as_group=os.getgid()),
                    )
                ],
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
            self._create_headless_service(podname, session, pod_uid)
        except self._api_error_v1_api as e:
            self.log.error(f'Couldn\'t create pod "{session}.{podname}": {e}')
            raise e

    def _get_process_uid(self, query: ProcessQuery):
        if not any([query.uuids, query.names, query.session, query.user]):
            return list(self.boot_request.keys())

        matched_uuids = set()
        for proc_uuid, boot_req in self.boot_request.items():
            meta = boot_req.process_description.metadata
            if any(uid.uuid == proc_uuid for uid in query.uuids):
                matched_uuids.add(proc_uuid)
            if query.session and query.session == meta.session:
                matched_uuids.add(proc_uuid)
            if query.user and query.user == meta.user:
                matched_uuids.add(proc_uuid)
            if any(re.search(name_reg, meta.name) for name_reg in query.names):
                matched_uuids.add(proc_uuid)
        return list(matched_uuids)

    def _logs_impl(self, log_request: LogRequest) -> LogLines:
        uuids = self._get_process_uid(log_request.query)
        uuid = self._ensure_one_process(uuids)
        podname = self.boot_request[uuid].process_description.metadata.name
        session = self.boot_request[uuid].process_description.metadata.session
        try:
            logs = self._core_v1_api.read_namespaced_pod_log(
                podname, session, tail_lines=log_request.how_far or 100
            )
            return LogLines(uuid=ProcessUUID(uuid=uuid), lines=logs.split('\n'))
        except self._api_error_v1_api as e:
            return LogLines(uuid=ProcessUUID(uuid=uuid), lines=[f"Could not retrieve logs: {e.reason}"])

    def _boot_impl(self, boot_request: BootRequest) -> ProcessInstance:
        return self.__boot(boot_request, str(uuid.uuid4()))

    def __boot(self, boot_request: BootRequest, uuid: str) -> ProcessInstance:
        session = boot_request.process_description.metadata.session
        podname = boot_request.process_description.metadata.name
        if uuid in self.boot_request:
            raise DruncCommandException(f'"{session}.{podname}":{uuid} already exists!')
        
        self.boot_request[uuid] = BootRequest()
        self.boot_request[uuid].CopyFrom(boot_request)

        self._create_namespace(session)
        self._create_pod(podname, session, boot_request)
        self._add_label(podname, "pod", "uuid", uuid, session=session)
        self.log.info(f'"{session}.{podname}":{uuid} boot request sent.')

        pd, pr, pu = ProcessDescription(), ProcessRestriction(), ProcessUUID(uuid=uuid)
        pd.CopyFrom(self.boot_request[uuid].process_description)
        pr.CopyFrom(self.boot_request[uuid].process_restriction)

        return ProcessInstance(
            process_description=pd, process_restriction=pr,
            status_code=ProcessInstance.StatusCode.RUNNING, uuid=pu,
        )

    def _ps_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        queried_uuids = self._get_process_uid(query)
        if not queried_uuids: return ProcessInstanceList()

        all_pods = self._core_v1_api.list_pod_for_all_namespaces(
            label_selector=self._get_creator_label_selector()
        )
        uuid_to_pod = {p.metadata.labels.get(f"uuid.{self.drunc_label}"): p for p in all_pods.items}
        
        ret = []
        for uuid in queried_uuids:
            if uuid not in self.boot_request: continue
            
            pod = uuid_to_pod.get(uuid)
            status_code = ProcessInstance.StatusCode.DEAD
            return_code = None
            if pod:
                if pod.status.phase == 'Running':
                    status_code = ProcessInstance.StatusCode.RUNNING
                elif pod.status.phase in ['Succeeded', 'Failed']:
                    if pod.status.container_statuses and pod.status.container_statuses[0].state.terminated:
                        return_code = pod.status.container_statuses[0].state.terminated.exit_code

            pd, pr, pu = ProcessDescription(), ProcessRestriction(), ProcessUUID(uuid=uuid)
            pd.CopyFrom(self.boot_request[uuid].process_description)
            pr.CopyFrom(self.boot_request[uuid].process_restriction)

            ret.append(ProcessInstance(
                process_description=pd, process_restriction=pr,
                status_code=status_code, return_code=return_code, uuid=pu,
            ))
        return ProcessInstanceList(values=ret)

    def _restart_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        uuids = self._get_process_uid(query)
        uuid = self._ensure_one_process(uuids)
        
        br_copy = BootRequest()
        br_copy.CopyFrom(self.boot_request[uuid])
        
        self._kill_impl(ProcessQuery(uuids=[ProcessUUID(uuid=uuid)]))
        ret = self.__boot(br_copy, uuid)
        return ProcessInstanceList(values=[ret])

    def _kill_pod(self, podname, session):
        try:
            self._core_v1_api.delete_namespaced_pod(podname, session, grace_period_seconds=0)
        except self._api_error_v1_api as e:
            if e.status != 404: raise e

    def _kill_if_empty_session(self, session):
        try:
            pods = self._core_v1_api.list_namespaced_pod(
                session, label_selector=self._get_creator_label_selector()
            )
            if not pods.items:
                self.log.info(f'Session "{session}" is empty, deleting namespace.')
                self._core_v1_api.delete_namespace(session)
        except self._api_error_v1_api as e:
            if e.status != 404:
                self.log.warning(f"Failed to check/delete empty session {session}: {e}")

    def _kill_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        ret, sessions_affected = [], set()
        uuids_to_kill = self._get_process_uid(query)
        
        for proc_uuid in uuids_to_kill:
            if proc_uuid not in self.boot_request: continue

            pd = self.boot_request[proc_uuid].process_description
            podname, session = pd.metadata.name, pd.metadata.session
            sessions_affected.add(session)
            self.log.info(f'Killing pod "{session}/{podname}" (UUID {proc_uuid})')
            
            try:
                self._kill_pod(podname, session)
            except Exception as e:
                self.log.error(f"Failed to issue kill for pod {podname}: {e}")
                continue
            
            pd_copy, pr_copy, pu_copy = ProcessDescription(), ProcessRestriction(), ProcessUUID(uuid=proc_uuid)
            pd_copy.CopyFrom(self.boot_request[proc_uuid].process_description)
            pr_copy.CopyFrom(self.boot_request[proc_uuid].process_restriction)
            
            ret.append(ProcessInstance(
                process_description=pd_copy, process_restriction=pr_copy,
                status_code=ProcessInstance.StatusCode.DEAD, uuid=pu_copy,
            ))
            del self.boot_request[proc_uuid]

        for session in sessions_affected:
            self._kill_if_empty_session(session)

        return ProcessInstanceList(values=ret)

    def _terminate_impl(self) -> ProcessInstanceList:
        self.log.info("Terminating all known K8s processes.")
        if not self.boot_request:
            self.log.info("No processes to terminate.")
            return ProcessInstanceList()
        return self._kill_impl(ProcessQuery(names=[".*"]))
    