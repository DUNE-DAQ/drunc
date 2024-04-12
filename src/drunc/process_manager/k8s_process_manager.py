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
        self._api_error_v1_api = client.rest.ApiException
        self._pod_v1_api = client.V1Pod
        self.drunc_label = "drunc.daq"

        ns = self._core_v1_api.list_namespace()
        for n in ns.items:
            if not self.drunc_label in n.metadata.labels.keys():
                continue
            print(n.metadata.name)
            ret = self._core_v1_api.list_namespaced_pod(n.metadata.name)
            for pod in ret.items:
                print(pod.metadata.name)
    
    def _label(self, obj, key, label):
        obj.metadata.labels.update({f"{key}.{self.drunc_label}": label})
        # add key 
        return obj
    
    def _add_creator_label(self, obj):
        return self._label(obj, "creator", self.__name__)
    
    def _create_namespace(self, nsname):
        ns = self._ns_v1_api(metadata=self._meta_v1_api(name=nsname))
        self._add_creator_label(ns)
        try:
            self._core_v1_api.create_namespace(ns)
            self._log.info(f"Creating \"{ns}\" namespace")            
        except Exception as e:
            self._log.error(f"Couldn't Create the Namespace: {e}")
            raise e


    def _create_pod(self, pod_name, ns):
        pod = self._pod_v1_api(metadata=self._meta_v1_api(name=pod_name,namespace=ns))
        self._add_creator_label(ns)
        try:
            self._core_v1_api.create_namespaced_pod(pod)
            self._log.info(f"Creating pod \"{pod_name}\" in \"{ns}\" namespace ")
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
        self._log.info(f'Booting {boot_request.process_description.metadata}')
        self._create_namespace("test")
        self._create_pod(boot_request.process_description.metadata.name, "test")
        pass
        return ProcessInstance()



    def _ps_impl(self, query:ProcessQuery, in_boot_request:bool=False) -> ProcessInstanceList:
        self._label(query, self.session)
        
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

        
        
        
        
        pass


    def _boot_impl(self, boot_request:BootRequest) -> ProcessUUID:
        import uuid
        this_uuid = str(uuid.uuid4())
        print(boot_request)


        return self.__boot(boot_request, this_uuid)
    



    def _restart_impl(self, query:ProcessQuery) -> ProcessInstanceList:
        uuids = self._get_process_uid(query, in_boot_request=True)
        uuid = self._ensure_one_process(uuids, in_boot_request=True)

        if uuid in self.process_store:
            process = self.process_store[uuid]
            if process.is_alive():
                process.terminate()

        return self.__boot(self.boot_request[uuid], uuid)


    def _kill_impl(self, query:ProcessQuery) -> ProcessInstanceList:
        pass