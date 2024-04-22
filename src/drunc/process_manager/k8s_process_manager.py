import grpc
import sh
from functools import partial
import threading
from kubernetes import client, config


from druncschema.process_manager_pb2 import BootRequest, ProcessQuery, ProcessUUID, ProcessMetadata, ProcessInstance, ProcessInstanceList, ProcessDescription, ProcessRestriction, LogRequest, LogLine
from drunc.process_manager.process_manager import ProcessManager

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
    
    # def _label(self, obj, key, label):
    #     obj.metadata.labels.update({f"{key}.{self.drunc_label}": label})
    #     return obj

    def _add_label(self, obj, key, label):
        body = {
            "metadata": {
                "labels": {
                    f"{key}.{self.drunc_label}": label
                }
            }
        }
        try:
            if isinstance(obj, self._ns_v1_api):
                self._core_v1_api.patch_namespace(obj.metadata.name, body)
                self._log.info(f"Added label \"{key}:{label}\" to namespace \"{obj.metadata.name}\"")
            elif isinstance(obj, self._pod_v1_api):
                self._core_v1_api.patch_namespaced_pod(obj.metadata.name, obj.metadata.namespace, body)
                self._log.info(f"Added label \"{key}:{label}\" to pod \"{obj.metadata.name}\" in \"{obj.metadata.namespace}\" namespace")
        except Exception as e:
            self._log.error(f"Couldn't add label to the object: {e}")
            raise e
    
    def _add_creator_label(self, obj):
        return self._add_label(obj, "creator", self.__class__.__name__)

    def _create_namespace(self, nsname):
        try:
            ns=self._core_v1_api.read_namespace(nsname)
            self._log.info(f"Namespace \"{nsname}\" already exists")
            self._add_creator_label(ns)
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
            pods=self._core_v1_api.create_namespaced_pod(ns, pod)
            self._log.info(f"Creating pod \"{pod_name}\" in \"{ns}\" namespace ")
            self._add_creator_label(pods)
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

        for uid in query.uuids:
            uuid_selector += [uid.uuid]

        processes = []
        all_the_uuids = self.process_store.keys() if not in_boot_request else self.boot_request.keys()

        for uuid in all_the_uuids:
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


    def __boot(self, boot_request:BootRequest, uuid:str) -> ProcessInstance:
        if boot_request is None:
            self._log.error('boot_request is None')
            return
        if ProcessInstance is None:
            self._log.error('ProcessInstance is None')
            return
        self._log.info(f'Booting {boot_request.process_description.metadata}')
        session = boot_request.process_description.metadata.session
        self._create_namespace(session)
        self._create_pod(boot_request.process_description.metadata.name, session)
        return ProcessInstance()



    def _ps_impl(self, query:ProcessQuery, in_boot_request:bool=False) -> ProcessInstanceList:
        #self._label(query, self.session)
        
        import re

        uuid_selector = []
        name_selector = query.names
        user_selector = query.user
        session_selector = query.session
        # relevant reading here: https://github.com/protocolbuffers/protobuf/blob/main/docs/field_presence.md

        for uid in query.uuids:
            uuid_selector += [uid.uuid]

        processes = []
        all_the_uuids = self.process_store.keys() if not in_boot_request else self.boot_request.keys()

        for uuid in all_the_uuids:
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


    def _boot_impl(self, boot_request:BootRequest) -> ProcessUUID:
        import uuid
        this_uuid = str(uuid.uuid4())
        return self.__boot(boot_request, this_uuid)
    



    def _restart_impl(self, query:ProcessQuery) -> ProcessInstanceList:
        uuids = self._get_process_uid(query, in_boot_request=True)
        uuid = self._ensure_one_process(uuids, in_boot_request=True)

        if uuid in self.process_store:
            process = self.process_store[uuid]
            if process.is_alive():
                process.terminate()

        return self.__boot(self.boot_request[uuid], uuid)


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