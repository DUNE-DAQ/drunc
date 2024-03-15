import grpc
import sh
from functools import partial
import threading
from kubernetes import client, config, watch 

from druncschema.process_manager_pb2 import BootRequest, ProcessQuery, ProcessUUID, ProcessMetadata, ProcessInstance, ProcessInstanceList, ProcessDescription, ProcessRestriction, LogRequest, LogLine
from drunc.process_manager.process_manager import ProcessManager

# # ------------------------------------------------
# # pexpect.spawn(...,preexec_fn=on_parent_exit('SIGTERM'))
from ctypes import cdll
import signal

# Constant taken from http://linux.die.net/include/linux/prctl.h
PR_SET_PDEATHSIG = 1

from drunc.exceptions import DruncException
class PrCtlError(DruncException):
    pass

class K8sProcessManager(ProcessManager):
    def __init__(self, pm_conf, **kwargs):
        
        import getpass
        self.session = getpass.getuser()
        super(K8sProcessManager, self).__init__(
            pm_conf = pm_conf,
            session = self.session,
            **kwargs
        )

        import getpass
        self.session = getpass.getuser() # unfortunate
        self.watchers = []

        self.pm = pm_conf
        self._log.debug("K8sProcessManager initialized")

    def create_namespace(self, request):
        self._log.debug("creating namespace")
        self._log.debug(request)
        namespace = request.metadata.name

        existing_namespaces = self.api.list_namespace().items
        namespaces_list = [ns.metadata.name for ns in existing_namespaces] 
        if namespace == namespaces_list:
            self._log.debug(f"failed to create namespace \"{namespace}\" because namespace already exists")
            return
        else:
            self._log.info(f"creating namespace \"{namespace}\" ")
            self.api.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)))

    
    def create_pod(self, request):
        pod_name = request.metadata.name
        image_name = request.metadata.image
        pod = client.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=client.V1ObjectMeta(name=pod_name),
            spec=client.V1PodSpec(
                containers=[
                    client.V1Container(
                        name=pod_name,
                        image=image_name,
                        ports=[client.V1ContainerPort(container_port=80)],
                    )
                ]
            ),
        )
    
    def delete_namespace(self, request):
        self._log.debug("deleting namespace")
        self._log.debug(request)
        namespace = request.metadata.name
        self.api.delete_namespace(namespace)
        self._log.info(f"deleted namespace \"{namespace}\" ")
        
    # def boot(self, request, context):     
    #     self._log.debug("booting process")
    #     self._log.debug(request)



    #     self.api.create_namespaced_pod(namespace=namespace, body=pod)