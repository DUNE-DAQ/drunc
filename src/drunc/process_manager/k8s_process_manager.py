import grpc
import sh
from functools import partial
import threading
from kubernetes import client, config
import time



from druncschema.process_manager_pb2 import BootRequest, ProcessQuery, ProcessUUID, ProcessMetadata, ProcessInstance, ProcessInstanceList, ProcessDescription, ProcessRestriction, LogRequest, LogLine
from drunc.process_manager.process_manager import ProcessManager
from drunc.exceptions import DruncCommandException





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
        self._apps_v1_api = client.AppsV1Api()
        self._ns_v1_api = client.V1Namespace
        self._meta_v1_api = client.V1ObjectMeta
        self._container_v1_api = client.V1Container
        self._api_error_v1_api = client.rest.ApiException
        self._pod_v1_api = client.V1Pod
        self._pod_spec_v1_api = client.V1PodSpec
        
        self.drunc_label = "drunc.daq"
    
    def is_alive(self, name, namespace):
        for _ in range(10):
            try:
                s = self._core_v1_api.read_namespaced_pod_status(name, namespace)
                for cond in s.status.conditions:
                    if cond.type == "Ready" and cond.status == "True":
                        return True
            except Exception as e:
                self._log.error(f"Error checking pod status: {e}")
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

    def _create_namespace(self, nsname):
        try:
            self._core_v1_api.read_namespace(nsname)
            self._log.info(f"Namespace \"{nsname}\" already exists")
            self._add_creator_label(nsname, 'namespace')
        except self._api_error_v1_api as e:
            if e.status == 404:
                namespace_manifest = {
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": nsname},
                }
                try:
                    self._core_v1_api.create_namespace(namespace_manifest)
                    self._log.info(f"Creating \"{nsname}\" namespace") 
                except Exception as e:
                    self._log.error(f"Couldn't Create the Namespace: {e}")
                    raise e
            else:
                raise e
        


    def _create_pod(self, pod_name, ns):
        pod = self._pod_v1_api(
            api_version="v1",
            kind="Pod",
            metadata=self._meta_v1_api(name=pod_name, namespace=ns),
            spec=self._pod_spec_v1_api(containers=[self._container_v1_api(name=pod_name,image="busybox",command=["sleep", "3600"])])
        )
        try:
            self._core_v1_api.create_namespaced_pod(ns, pod)
            self._log.info(f"Creating pod \"{pod_name}\" in \"{ns}\" namespace ")
            self._add_creator_label(pod_name, 'pod')
        except Exception as e:
            self._log.error(f"Couldn't Create pod with name: \"{pod_name}\": {e}")
            raise e

    def _get_pod_logs(self, pod_name, namespace):
        config.load_kube_config()
        log=self._core_v1_api.read_namespaced_pod_log(name=pod_name, namespace=namespace)
        return log


    def _get_process_uid(self, query:ProcessQuery, in_boot_request:bool=False):
        import re

        uuid_selector = []
        name_selector = query.names
        user_selector = query.user
        session_selector = query.session
        # relevant reading here: https://github.com/protocolbuffers/protobuf/blob/main/docs/field_presence.md

        processes = []

        all_the_uuids = self.boot_request.keys()
        
        all_pods = []
        for uuid in all_the_uuids:
            podname = self.boot_request[uuid].process_description.metadata.name
            all_pods.append(podname)
        self._log.info(f'podnames: {all_pods}')

        self._log.info(f'all_the_uuids: {all_the_uuids}')

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
    

# --------------------------------------Commands--------------------------------------


    def _terminate(self):

        self._log.info('Terminating')



    async def _logs_impl(self, log_request:LogRequest) -> LogLine:
        pass

    def _boot_impl(self, boot_request:BootRequest) -> ProcessUUID:
        import uuid
        this_uuid = str(uuid.uuid4())
        self._log.info(f'uuid for booting is:{this_uuid}')
        return self.__boot(boot_request, this_uuid)
    

    def __boot(self, boot_request:BootRequest, uuid:str) -> ProcessInstance:
        session = boot_request.process_description.metadata.session
        podnames = boot_request.process_description.metadata.name

        self._log.info(f'boot_request: {boot_request}')
        if uuid in self.boot_request:
            raise DruncCommandException(f'Process {uuid} already exists!')

        self.boot_request[uuid] = BootRequest()
        self.boot_request[uuid].CopyFrom(boot_request)

        for uuid in self.boot_request:
            self._log.info(f'boot_request1: {self.boot_request[uuid]}')
        
        self._log.info(f'Booting \n{boot_request.process_description.metadata}')

        self._create_namespace(session)
        self._create_pod(podnames, session)
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


        # return_code = None
        # try:
        #     if not self.process_store[uuid].is_alive():
        #         return_code = self.process_store[uuid].exit_code
        #     else:
        #         alive = True
        # except Exception as e:
        #     pass

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
            status_code = ProcessInstance.StatusCode.RUNNING, #if self.is_alive(podname,session) else ProcessInstance.StatusCode.DEAD,
            uuid = pu
        )

        self.boot_request[uuid]=pi

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

            return_code = None
            if not self.is_alive(podname, session):
                try:
                    return_code = self.boot_request[uuid].exit_code
                except Exception as e:
                    pass
            
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
        # uuids = self._get_process_uid(query, in_boot_request=True)
        # uuid = self._ensure_one_process(uuids, in_boot_request=True)

        # if uuid in self.process_store:
        #     process = self.process_store[uuid]
        #     if process.is_alive():
        #         process.terminate()

        return #self.__boot(self.boot_request[uuid], uuid)
    

    # ORDER MATTERS!
    # @broadcasted # outer most wrapper 1st step
    # @authentified_and_authorised(
    #     action=ActionType.DELETE,
    #     system=SystemType.PROCESS_MANAGER
    # ) # 2nd step
    # @unpack_request_data_to(ProcessQuery) # 3rd step
    # @pack_response # 4th step
    # def flush(self, query:ProcessQuery) -> Response:
    #     return


    def _kill_impl(self,query:ProcessQuery) -> ProcessInstanceList: #
        namespace = "tiago"
        try:
            self._core_v1_api.delete_namespace(namespace)
            self._log.info(f"Killing namespace \"{namespace}\"")
        except self._api_error_v1_api as e:
            if e.status == 404:
                self._log.error(f"Namespace \"{namespace}\" does not exist")
            else:
                self._log.error(f"Couldn't kill namespace \"{namespace}\": {e}")
                raise e
        return ProcessInstanceList()