from drunc_core.utils.utils import get_logger
from drunc_messages.process_orchestrator_pb2 import ProcessOrchestratorTypes

from drunc.process_orchestrator.k8s import K8sProcessOrchestrator
from drunc.process_orchestrator.ssh import SSHProcessOrchestrator


def get_process_orchestrator(conf, **kwargs):
    log = get_logger("process_orchestrator.factory")

    if conf.data.type == ProcessOrchestratorTypes.SSH:
        log.info("Starting [green]SSH process_orchestrator[/green]")
        return SSHProcessOrchestrator(conf, **kwargs)
    elif conf.data.type == ProcessOrchestratorTypes.K8s:
        log.info("Starting [green]K8s process_orchestrator[/green]")
        return K8sProcessOrchestrator(conf, **kwargs)
    else:
        log.error(f"ProcessOrchestrator type {conf.get('type')} is unsupported!")
        raise RuntimeError(
            f"ProcessOrchestrator type {conf.get('type')} is unsupported!"
        )
