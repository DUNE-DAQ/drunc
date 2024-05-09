
from druncschema.process_manager_pb2 import BootRequest, ProcessQuery, ProcessUUID, ProcessInstance, ProcessInstanceList, ProcessDescription, ProcessRestriction, LogRequest, LogLine
from druncschema.request_response_pb2 import Response
from druncschema.authoriser_pb2 import ActionType, SystemType

from drunc.process_manager.process_manager import ProcessManager
from drunc.exceptions import DruncCommandException, DruncException
from drunc.k8s_exceptions import DruncK8sNamespaceAlreadyExists
from drunc.authoriser.decorators import authentified_and_authorised
from drunc.broadcast.server.decorators import broadcasted
from drunc.utils.grpc_utils import unpack_request_data_to, pack_response



class K8sProcessManager(ProcessManager):
    def __init__(self, configuration, **kwargs):

        import getpass
        self.session = getpass.getuser() # unfortunate

        super().__init__(
            configuration = configuration,
            session = self.session,
            **kwargs
        )

        from kubernetes import client, config

        import logging
        self._log = logging.getLogger('k8s-process-manager')
        config.load_kube_config()

        self._k8s_client = client
        self._core_v1_api = client.CoreV1Api()

        self._ns_v1_api = client.V1Namespace
        self._meta_v1_api = client.V1ObjectMeta
        self._container_v1_api = client.V1Container
        self._api_error_v1_api = client.rest.ApiException
        self._pod_v1_api = client.V1Pod
        self._pod_spec_v1_api = client.V1PodSpec
        self._pod_security_context_v1_api = client.V1PodSecurityContext
        self._security_context_v1_api = client.V1SecurityContext
        self._capabilities_v1_api = client.V1Capabilities

        self.drunc_label = "drunc.daq"

        namespaces = self._core_v1_api.list_namespace(
            label_selector=f"creator.{self.drunc_label}={self.__class__.__name__}"
        )

        namespace_names = [ns.metadata.name for ns in namespaces.items]
        namespace_list_str = "\n - ".join(namespace_names)

        if namespace_list_str:
            namespace_list_str = "\n - "+namespace_list_str
            self._log.info(f"Active namespaces created by drunc:{namespace_list_str}")

        else:
            self._log.info(f"No active namespace created by drunc")


    def is_alive(self, podname, session):
        pods = self._core_v1_api.list_namespaced_pod(session)
        pod_names = [pod.metadata.name for pod in pods.items]
        if podname in pod_names:
            for _ in range(10):
                s = self._core_v1_api.read_namespaced_pod_status(podname, session)
                for cond in s.status.conditions:
                    if cond.type == "Ready" and cond.status == "True":
                        return True
        else:
            return False


    def _add_label(self, obj_name, obj_type, key, label):
        body = {
            "metadata": {
                "labels": {
                    f"{key}.{self.drunc_label}":label
                }
            }
        }

        if obj_type == 'namespace':
            self._core_v1_api.patch_namespace(obj_name, body)
            self._log.info(f"Added label \"{key}.{self.drunc_label}:{label}\" to \"{obj_name}\" session")
        elif obj_type == 'pod':
            session = self._get_pod_namespace(obj_name)
            self._core_v1_api.patch_namespaced_pod(obj_name, session, body)
            self._log.info(f"Added label \"{key}.{self.drunc_label}:{label}\" to \"{session}.{obj_name}\"")
        else:
            raise DruncException(f'Cannot add label to object type: {obj_type}')


    def _get_pod_namespace(self, podname):
        pods = self._core_v1_api.list_pod_for_all_namespaces()
        for pod in pods.items:
            if pod.metadata.name == podname:
                return pod.metadata.namespace

    def _add_creator_label(self, obj_name, obj_type):
        return self._add_label(obj_name, obj_type, "creator", self.__class__.__name__)

    def _get_creator_label_selector(self):
        label = f"creator.{self.drunc_label}={self.__class__.__name__}"
        return label

    def _create_namespace(self, session):
        ns_list = self._core_v1_api.list_namespace(
            label_selector = self._get_creator_label_selector(),
        )
        ns_names = [ns.metadata.name for ns in ns_list.items]

        if not session in ns_names:
            self._log.info(f"Creating \"{session}\" session")
            namespace_manifest = {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": session,
                    "labels": {
                        "pod-security.kubernetes.io/enforce":"privileged",
                        "pod-security.kubernetes.io/enforce-version":"latest",
                        "pod-security.kubernetes.io/warn":"privileged",
                        "pod-security.kubernetes.io/warn-version":"latest",
                        "pod-security.kubernetes.io/audit":"privileged",
                        "pod-security.kubernetes.io/audit-version":"latest"
                        }
                    },
                }
            self._core_v1_api.create_namespace(namespace_manifest)
            self._add_creator_label(session, 'namespace')
        else:
            return DruncK8sNamespaceAlreadyExists (f'\"{session}\" session already exists')


    def _volume(self, name, host_path):
        return self._k8s_client.V1Volume(
            name=name,
            host_path=self._k8s_client.V1HostPathVolumeSource(path=host_path)
        )
    
    def _volume_mount(self, name, mount_path):
        return self._k8s_client.V1VolumeMount(
            name=name,
            mount_path=mount_path
        )

    def _env_vars(self, env_var):
        env_vars_list = [self._k8s_client.models.V1EnvVar(name=k, value=v) for k, v in env_var.items()]
        return env_vars_list

    def _execs_and_args(self, executable_and_arguments=[]):
        exec_and_args = []
        for e_and_a in executable_and_arguments:
            args = [a for a in e_and_a.args]
            exec_and_args += [" ".join([e_and_a.exec] + args)]
            # exec_and_args = ["source rte", "daq_application --something argument"]
        exec_and_args = '; '.join(exec_and_args)
        return exec_and_args

    def _create_pod(self, podname, session, boot_request:BootRequest):
        import os
        # HACK
        import socket
        hostname = socket.gethostname()
        #/ HACK

        # pod_image = boot_request.{where_pod_image_is}
        pod_image="ghcr.io/dune-daq/alma9:latest"

        pod = self._pod_v1_api(
            api_version="v1",
            kind="Pod",
            metadata=self._meta_v1_api(
                name=podname,
                namespace=session
            ),
            spec=self._pod_spec_v1_api(
                restart_policy="Never",
                containers=[
                    self._container_v1_api(
                        name=podname,
                        image=pod_image,
                        command = ["sh"],
                        #args = ["-c", "exit 3"],
                        args = ["-c", self._execs_and_args(boot_request.process_description.executable_and_arguments)],
                        #args = ["-c", "sleep 3600"],
                        env= self._env_vars(boot_request.process_description.env),
                        volume_mounts=[self._volume_mount("pwd",os.getcwd()), self._volume_mount("cvmfs","/cvmfs/")],
                        working_dir = boot_request.process_description.process_execution_directory,
                        restart_policy="Never",
                        security_context = self._security_context_v1_api(
                            run_as_user = os.getuid(),
                            run_as_group = os.getgid(),
                            run_as_non_root = True,
                            allow_privilege_escalation = False,
                            capabilities = self._capabilities_v1_api(
                                drop = ["ALL"]
                            ),
                            seccomp_profile = self._k8s_client.V1SeccompProfile(
                                type = "RuntimeDefault"
                            )
                        ),
                    )
                ],
                volumes=[self._volume("pwd",os.getcwd()), self._volume("cvmfs","/cvmfs/")],
                # HACK
                affinity = self._k8s_client.V1Affinity(
                    self._k8s_client.V1NodeAffinity(
                        required_during_scheduling_ignored_during_execution = self._k8s_client.V1NodeSelector(
                            node_selector_terms = [
                                self._k8s_client.V1NodeSelectorTerm(
                                    match_expressions = [
                                        {
                                            'key': "kubernetes.io/hostname",
                                            "operator": 'In',
                                            'values': [hostname],
                                        }
                                    ]
                                )
                            ]
                        )
                    )
                ) if hostname.startswith('np04') else None,
                #/ HACK

                security_context = self._pod_security_context_v1_api(
                    run_as_user = os.getuid(),
                    run_as_group = os.getgid(),
                    run_as_non_root = True,
                ),
            )
        )
        try:
            self._core_v1_api.create_namespaced_pod(session, pod)
            self._log.info(f"Creating \"{session}.{podname}\"")
            self._add_creator_label(podname, 'pod')
        except Exception as e:
            self._log.error(f"Couldn't create pod with name: \"{session}.{podname}\": {e}")
            raise e


    def _get_process_uid(self, query:ProcessQuery, in_boot_request:bool=False):
        import re
        uuid_selector = []
        name_selector = query.names
        user_selector = query.user
        session_selector = query.session
        # relevant reading here: https://github.com/protocolbuffers/protobuf/blob/main/docs/field_presence.md

        processes = []

        all_the_uuids = self.boot_request.keys()

        for uid in query.uuids:
            uuid_selector += [uid.uuid]

        all_pods = []
        for uuid in all_the_uuids:
            podname = self.boot_request[uuid].process_description.metadata.name
            all_pods.append(podname)

        for uuid in self.boot_request.keys():
            accepted = False
            meta = self.boot_request[uuid].process_description.metadata

            if uuid in uuid_selector: accepted = True

            for name_reg in name_selector:
                if re.search(name_reg, meta.name):
                    accepted = True

            if session_selector == meta.session: accepted = True

            if user_selector == meta.user: accepted = True

            if accepted: processes.append(uuid)

        return processes

    def _get_pi(self, uuid, podname, session, return_code=None):
        pd = ProcessDescription()
        pd.CopyFrom(self.boot_request[uuid].process_description)
        pr = ProcessRestriction()
        pr.CopyFrom(self.boot_request[uuid].process_restriction)
        pu = ProcessUUID(uuid=uuid)

        pi = ProcessInstance(
            process_description = pd,
            process_restriction = pr,
            status_code = ProcessInstance.StatusCode.RUNNING if self.is_alive(podname, session) else ProcessInstance.StatusCode.DEAD,
            return_code = return_code,
            uuid = pu
        )
        return pi

    def _kill_pod(self, podname, session):
        pods = self._core_v1_api.list_namespaced_pod(session)
        pod_names = [pod.metadata.name for pod in pods.items]
        if podname in pod_names:
            self._core_v1_api.delete_namespaced_pod(podname, session,grace_period_seconds=0)
            while podname in pod_names:
                from time import sleep
                sleep(1)
                pods = self._core_v1_api.list_namespaced_pod(session)
                pod_names = [pod.metadata.name for pod in pods.items]
        else:
            pass


    def _kill_if_empty_session(self,session):
        pods = self._core_v1_api.list_namespaced_pod(
            session,
            label_selector = self._get_creator_label_selector(),
        )
        deletion_timestamps = [pod.metadata.deletion_timestamp for pod in pods.items]
        if not pods.items or all(timestamp is not None for timestamp in deletion_timestamps):
            self._log.info(f"Killing empty \"{session}\" session")
            self._core_v1_api.delete_namespace(session)

            ns_list = self._core_v1_api.list_namespace(
                label_selector = self._get_creator_label_selector(),
            )
            namespaces = [ns.metadata.name for ns in ns_list.items]
            while session in namespaces:
                from time import sleep
                sleep(1)
                ns_list = self._core_v1_api.list_namespace(
                    label_selector = self._get_creator_label_selector(),
                )
                namespaces = [ns.metadata.name for ns in ns_list.items]
            else:
                self._log.info(f"\"{session}\" session has been killed")
                pass
        else:
            pass

    def _return_code(self, podname, session):
        pods = self._core_v1_api.list_namespaced_pod(session)
        pod_names = [pod.metadata.name for pod in pods.items]
        if not podname in pod_names:
            return_code = None
        else:
            if not self.is_alive(podname, session):
                return_code = self._core_v1_api.read_namespaced_pod_status(podname, session).status.container_statuses[0].state.terminated.exit_code
            else:
                return_code = None
        return return_code



# --------------------------------------Commands--------------------------------------


    def _terminate(self):

        self._log.info('Terminating')



    async def _logs_impl(self, log_request:LogRequest) -> LogLine:

        uuids = self._get_process_uid(log_request.query, in_boot_request=True)
        uuid = self._ensure_one_process(uuids, in_boot_request=True)
        for uuid in self._get_process_uid(log_request.query):
            podname = self.boot_request[uuid].process_description.metadata.name
            session = self.boot_request[uuid].process_description.metadata.session
            for log in self._core_v1_api.read_namespaced_pod_log(podname, session, tail_lines=log_request.how_far).split("\n"):
                yield LogLine(line=log)



    def _boot_impl(self, boot_request:BootRequest) -> ProcessUUID:
        import uuid
        this_uuid = str(uuid.uuid4())
        return self.__boot(boot_request, this_uuid)


    def __boot(self, boot_request:BootRequest, uuid:str) -> ProcessInstance:

        session = boot_request.process_description.metadata.session
        podnames = boot_request.process_description.metadata.name
        if uuid in self.boot_request:
            raise DruncCommandException(f'\"{session}.{podnames}\":{uuid} already exists!')
        self.boot_request[uuid] = BootRequest()
        self.boot_request[uuid].CopyFrom(boot_request)

        self._create_namespace(session)
        self._create_pod(podnames, session, boot_request)
        self._add_label(podnames, 'pod', 'uuid', uuid)
        self._log.info(f'\"{session}.{podnames}\":{uuid} booted')

        pods = self._core_v1_api.list_namespaced_pod(session)
        pod_names = [pod.metadata.name for pod in pods.items]

        for _ in range(10):
            if not podnames in pod_names:
                from time import sleep
                sleep(1)
            else:
                break
        else:
            raise DruncException(f"Not able to boot \"{session}.{podnames}\":{uuid}")
        return self._get_pi(uuid, podnames, session)



    def _ps_impl(self, query:ProcessQuery, in_boot_request:bool=False) -> ProcessInstanceList:
        ret=[]
        for uuid in self._get_process_uid(query):
            podname = self.boot_request[uuid].process_description.metadata.name
            session = self.boot_request[uuid].process_description.metadata.session

            return_code = self._return_code(podname, session)

            ret.append(self._get_pi(uuid, podname, session, return_code))

        pil = ProcessInstanceList(
            values=ret
        )

        return pil



    def _restart_impl(self, query:ProcessQuery) -> ProcessInstanceList:
        #ret = []
        uuids = self._get_process_uid(query, in_boot_request=True)
        uuid = self._ensure_one_process(uuids, in_boot_request=True)
        for uuid in self._get_process_uid(query):
            podname = self.boot_request[uuid].process_description.metadata.name
            session = self.boot_request[uuid].process_description.metadata.session
            self._log.info(f'Restarting \"{session}.{podname}\":{uuid}')

            self._kill_pod(podname, session)

            same_uuid_br = []
            same_uuid_br = BootRequest()
            same_uuid_br.CopyFrom(self.boot_request[uuid])
            same_uuid = uuid

            del self.boot_request[uuid]
            del uuid

            ret=self.__boot(same_uuid_br, same_uuid)

            del same_uuid_br
            del same_uuid

            # ret.append(self._get_pi(uuid, podname, session))

        # pil = ProcessInstanceList(
        #     values=ret
        # )
        return ret


    # ORDER MATTERS!
    @broadcasted # outer most wrapper 1st step
    @authentified_and_authorised(
        action=ActionType.DELETE,
        system=SystemType.PROCESS_MANAGER
    ) # 2nd step
    @unpack_request_data_to(ProcessQuery) # 3rd step
    @pack_response # 4th step
    def flush(self, query:ProcessQuery) -> Response:
        ret=[]
        self._log.info(f'Flushing dead processes')
        for uuid in self._get_process_uid(query):
            podname = self.boot_request[uuid].process_description.metadata.name
            session = self.boot_request[uuid].process_description.metadata.session

            if not self.is_alive(podname, session):
                return_code = self._return_code(podname, session)
                ret.append(self._get_pi(uuid, podname, session, return_code))
                self._log.info(f'Flushing \"{session}.{podname}\":{uuid}')
                del self.boot_request[uuid]
                self._log.info(f'\"{session}.{podname}\":{uuid} flushed')
                del uuid
                self._kill_pod(podname, session)

            self._kill_if_empty_session(session)

        pil = ProcessInstanceList(
            values=ret
        )


        return pil


    def _kill_impl(self, query:ProcessQuery, in_boot_request:bool=False) -> ProcessInstanceList:
        ret=[]
        for uuid in self._get_process_uid(query):
            podname = self.boot_request[uuid].process_description.metadata.name
            session = self.boot_request[uuid].process_description.metadata.session

            self._log.info(f'Killing \"{session}.{podname}\":{uuid}')

            self._kill_pod(podname, session)

            self._log.info(f'\"{session}.{podname}\":{uuid} killed')


            return_code = self._return_code(podname, session)
            ret.append(self._get_pi(uuid, podname, session, return_code))

            del self.boot_request[uuid]
            del uuid

            self._kill_if_empty_session(session)

        pil = ProcessInstanceList(
            values=ret
        )
        return pil
