from kubernetes import client, config

from druncschema.process_manager_pb2 import BootRequest, ProcessQuery, ProcessUUID, ProcessInstance, ProcessInstanceList, ProcessDescription, ProcessRestriction, LogRequest, LogLine
from druncschema.request_response_pb2 import Response
from druncschema.authoriser_pb2 import ActionType, SystemType

from drunc.process_manager.process_manager import ProcessManager
from drunc.exceptions import DruncCommandException, DruncException, DruncK8sNamespaceAlreadyExists
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

        import logging
        self._log = logging.getLogger('k8s-process-manager')
        config.load_kube_config()

        self._core_v1_api = client.CoreV1Api()

        self._ns_v1_api = client.V1Namespace
        self._meta_v1_api = client.V1ObjectMeta
        self._container_v1_api = client.V1Container
        self._api_error_v1_api = client.rest.ApiException
        self._pod_v1_api = client.V1Pod
        self._pod_spec_v1_api = client.V1PodSpec

        self.drunc_label = "drunc.daq"

        namespaces = self._core_v1_api.list_namespace(
            label_selector=self._get_creator_label_selector()
        )

        namespace_names = [ns.metadata.name for ns in namespaces.items]
        namespace_list_str = "\n - ".join(namespace_names)

        if namespace_list_str:
            namespace_list_str = "\n - "+namespace_list_str
            self._log.info(f"Active namespaces created by drunc:{namespace_list_str}")

        else:
            self._log.info(f"No active namespace created by drunc")


    def is_alive(self, name, namespace):
        try:
            for _ in range(10):
                s = self._core_v1_api.read_namespaced_pod_status(name, namespace)
                for cond in s.status.conditions:
                    if cond.type == "Ready" and cond.status == "True":
                        return True
        except self._api_error_v1_api as e:
            if e.status == 404:
                return False
        return False

    def _add_label(self, obj_name, obj_type, key, label):
        body = {
            "metadata": {
                "labels": {
                    f"{key}.{self.drunc_label}": label
                }
            }
        }
        try:
            if obj_type == 'namespace':
                self._core_v1_api.patch_namespace(obj_name, body)
                self._log.info(f"Added label \"{key}:{label}\" to namespace \"{obj_name}\"")
            elif obj_type == 'pod':
                namespace = self._get_pod_namespace(obj_name)
                self._core_v1_api.patch_namespaced_pod(obj_name, namespace, body)
                self._log.info(f"Added label \"{key}:{label}\" to pod \"{obj_name}\" in \"{namespace}\" namespace")
            else:
                raise DruncException(f'Cannot add label to object type: {obj_type}')
        except Exception as e:
            self._log.error(f"Couldn't add label to the object: {e}")
            raise e

    def _get_pod_namespace(self, pod_name):
        pods = self._core_v1_api.list_pod_for_all_namespaces(watch=False)
        for pod in pods.items:
            if pod.metadata.name == pod_name:
                return pod.metadata.namespace
        return None

    def _add_creator_label(self, obj_name, obj_type):
        return self._add_label(obj_name, obj_type, "creator", self.__class__.__name__)

    def _get_creator_label_selector(self):
        return f"creator.{self.drunc_label}={self.__class__.__name__}"

    def _create_namespace(self, nsname):
        ns_list = self._core_v1_api.list_namespace()

        ns_names = [ns.metadata.name for ns in ns_list.items]
        self._log.info(f'ns_list: {ns_names}')
        if not nsname in ns_names:
            namespace_manifest = {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": nsname},
            }
            self._core_v1_api.create_namespace(namespace_manifest)
            self._add_creator_label(nsname, 'namespace')
        else:
            return DruncK8sNamespaceAlreadyExists (f'Session {nsname} already exists')




    def _create_pod(self, pod_name, ns, pod_image="busybox",env_vars=None):
        pod = self._pod_v1_api(
            api_version="v1",
            kind="Pod",
            metadata=self._meta_v1_api(name=pod_name, namespace=ns),
            spec=self._pod_spec_v1_api(containers=[self._container_v1_api(name=pod_name,image=pod_image,command=["sleep", "3600"],env=env_vars)])
        )
        try:
            self._core_v1_api.create_namespaced_pod(ns, pod)
            self._log.info(f"Creating pod \"{pod_name}\" in \"{ns}\" namespace ")
            self._add_creator_label(pod_name, 'pod')
        except Exception as e:
            self._log.error(f"Couldn't Create pod with name: \"{pod_name}\": {e}")
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

    def _kill_if_empty_session(self,session):
        pods = self._core_v1_api.list_namespaced_pod(session)
        deletion_timestamps = [pod.metadata.deletion_timestamp for pod in pods.items]
        if not pods.items or all(timestamp is not None for timestamp in deletion_timestamps):
            self._log.info(f"All pods in \"{session}\" namespace are terminating")
            self._core_v1_api.delete_namespace(session)




# --------------------------------------Commands--------------------------------------


    def _terminate(self):

        self._log.info('Terminating')



    async def _logs_impl(self, log_request:LogRequest) -> LogLine:
        pass

    def _boot_impl(self, boot_request:BootRequest) -> ProcessUUID:
        import uuid
        this_uuid = str(uuid.uuid4())
        return self.__boot(boot_request, this_uuid)


    def __boot(self, boot_request:BootRequest, uuid:str) -> ProcessInstance:
        session = boot_request.process_description.metadata.session
        podnames = boot_request.process_description.metadata.name

        if uuid in self.boot_request:
            raise DruncCommandException(f'Process {uuid} already exists!')
        self.boot_request[uuid] = BootRequest()
        self.boot_request[uuid].CopyFrom(boot_request)

        env_var = boot_request.process_description.env
        env_vars_list = [client.models.V1EnvVar(name=k, value=v) for k, v in env_var.items()]


        self._create_namespace(session)
        self._create_pod(podnames, session, env_vars=env_vars_list)
        self._add_label(podnames, 'pod', 'uuid', uuid)

        pd = ProcessDescription()
        pd.CopyFrom(self.boot_request[uuid].process_description)
        pr = ProcessRestriction()
        pr.CopyFrom(self.boot_request[uuid].process_restriction)
        pu = ProcessUUID(uuid=uuid)

        # while not self.is_alive(podname, session):
        #     for _ in range(20):
        #         if self.is_alive(podname, session):
        #             break
        #         time.sleep(1)
        #     else:
        #         raise Exception("Timeout: Pod did not boot within 20 seconds")


        # if self.is_alive(podname, session):
            # return_code = self._core_v1_api.read_namespaced_pod_status(podname, session).status.container_statuses[0].state.terminated
        # else:
            # return_code = 404

        # if uuid not in self.process_store:
        #     pi = ProcessInstance(
        #         process_description = pd,
        #         process_restriction = pr,
        #         status_code = ProcessInstance.StatusCode.DEAD, ## should be unknown
        #         return_code = return_code,
        #         uuid = pu
        #     )
        #     return pi

        # self._log.info(f'process_store: {self.process_store[uuid].is_alive()}')

        pi = ProcessInstance(
            process_description = pd,
            process_restriction = pr,
            status_code = ProcessInstance.StatusCode.RUNNING if self.is_alive(podnames,session) else ProcessInstance.StatusCode.DEAD,
            uuid = pu
        )



        return pi



    def _ps_impl(self, query:ProcessQuery, in_boot_request:bool=False) -> ProcessInstanceList:
        ret = []

        for uuid in self._get_process_uid(query):
            podname = self.boot_request[uuid].process_description.metadata.name
            session = self.boot_request[uuid].process_description.metadata.session

            pd = ProcessDescription()
            pd.CopyFrom(self.boot_request[uuid].process_description)
            pr = ProcessRestriction()
            pr.CopyFrom(self.boot_request[uuid].process_restriction)
            pu = ProcessUUID(uuid=uuid)

            if self.is_alive(podname, session):
                return_code = self._core_v1_api.read_namespaced_pod_status(podname, session).status.container_statuses[0].state.terminated
            else:
                return_code = 404

            pi = ProcessInstance(
                process_description = pd,
                process_restriction = pr,
                status_code = ProcessInstance.StatusCode.RUNNING if self.is_alive(podname, session) else ProcessInstance.StatusCode.DEAD,
                return_code = return_code,
                uuid = pu
            )
            ret.append(pi)

        pil = ProcessInstanceList(
            values=ret
        )

        return pil



    def _restart_impl(self, query:ProcessQuery) -> ProcessInstanceList:
        uuids = self._get_process_uid(query, in_boot_request=True)
        uuid = self._ensure_one_process(uuids, in_boot_request=True)
        for uuid in self._get_process_uid(query):
            podname = self.boot_request[uuid].process_description.metadata.name
            session = self.boot_request[uuid].process_description.metadata.session
            self._log.info(f'Restarting pod \"{podname}\" in \"{session}\" namespace')
            try:
                self._core_v1_api.delete_namespaced_pod(podname, session,grace_period_seconds=1)
            except self._api_error_v1_api as e:
                if e.status == 404:
                    self._log.error(f"Namespace \"{session}\" does not exist")
                else:
                    self._log.error(f"Couldn't kill pod \"{podname}.{session}\": {e}")
                    raise e

            same_uuid_br = []
            same_uuid_br = BootRequest()
            same_uuid_br.CopyFrom(self.boot_request[uuid])
            same_uuid = uuid
            self._log.info(f'uuid: {same_uuid}')

            del self.boot_request[uuid]
            del uuid

            while True:
                try:
                    self._core_v1_api.read_namespaced_pod(podname, session)
                    from time import sleep
                    sleep(1)
                except self._api_error_v1_api as e:
                    if e.status == 404:
                        break
                    else:
                        self._log.error(f"Error while waiting for pod \"{podname}.{session}\" to terminate: {e}")
                        continue

            ret=self.__boot(same_uuid_br, same_uuid)
            self._log.info(f'{same_uuid} Process restarted')



            # pd = ProcessDescription()
            # pd.CopyFrom(same_uuid_br.process_description)
            # pr = ProcessRestriction()
            # pr.CopyFrom(same_uuid_br.process_restriction)
            # pu = ProcessUUID(uuid=same_uuid)

            # if self.is_alive(podname, session):
                # return_code = self._core_v1_api.read_namespaced_pod_status(podname, session).status.container_statuses[0].state.terminated
            # else:
                # return_code = 404

            # pi = ProcessInstance(
            #     process_description = pd,
            #     process_restriction = pr,
            #     status_code = ProcessInstance.StatusCode.RUNNING if self.is_alive(podname, session) else ProcessInstance.StatusCode.DEAD,
            #     return_code = return_code,
            #     uuid = pu
            # )
            # ret.append(pi)

            del same_uuid_br
            del same_uuid

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
        for uuid in self._get_process_uid(query):
            podname = self.boot_request[uuid].process_description.metadata.name
            session = self.boot_request[uuid].process_description.metadata.session
            self._log.info(f'Flushing pod \"{podname}\" in \"{session}\" namespace')

            if self.is_alive(podname, session):
                return_code = self._core_v1_api.read_namespaced_pod_status(podname, session).status.container_statuses[0].state.terminated
            else:
                return_code = 404

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
            ret.append(pi)
            if not self.is_alive(podname, session):
                del self.boot_request[uuid]
                del uuid

            self._kill_if_empty_session(session)

        pil = ProcessInstanceList(
            values=ret
        )


        return pil


    def _kill_impl(self, query:ProcessQuery, in_boot_request:bool=False) -> ProcessInstanceList:
        ret = []
        self._log.info(f'query: {query.session}')

        for uuid in self._get_process_uid(query):
            podname = self.boot_request[uuid].process_description.metadata.name
            session = self.boot_request[uuid].process_description.metadata.session
            self._log.info(f'Killing pod \"{podname}\" in \"{session}\" namespace')
            try:
                self._core_v1_api.delete_namespaced_pod(podname,session, grace_period_seconds=1)
            except self._api_error_v1_api as e:
                if e.status == 404:
                    return
                else:
                    self._log.error(f"Couldn't kill pod \"{podname}.{session}\": {e}")
                    raise e

            if self.is_alive(podname, session):
                return_code = self._core_v1_api.read_namespaced_pod_status(podname, session).status.container_statuses[0].state.terminated
            else:
                return_code = 404

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
            ret.append(pi)
            del self.boot_request[uuid]
            del uuid

            self._kill_if_empty_session(session)

        pil = ProcessInstanceList(
            values=ret
        )


        return pil
