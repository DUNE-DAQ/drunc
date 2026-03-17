"""
Concrete implementation of ProcessManager for testing purposes.

This class provides dummy implementations of all abstract _impl_ methods in the ProcessManager
base class, allowing for testing serialisation/deserialisation. The request-handling endpoints are real.

"""

from typing import Optional
from unittest.mock import Mock

from druncschema.process_manager_pb2 import (
    BootRequest,
    LogLines,
    LogRequest,
    ProcessInstanceList,
    ProcessQuery,
)

from drunc.process_manager.configuration import (
    ProcessManagerConfHandler,
)
from drunc.process_manager.process_manager import ProcessManager, ResponseFlag


class ConcreteProcessManager(ProcessManager):
    """
    Concrete implementation of ProcessManager with dummy _impl_ functions.
    """

    def __init__(
        self,
        configuration: ProcessManagerConfHandler = Mock(),
        name: str = "process_manager_no_impl",
        session: Optional[str] = None,
        **kwargs,
    ):
        """
        all-default constructor for testing purposes.
        """
        configuration.get_data().opmon_publisher = None
        super().__init__(configuration, name, session, **kwargs)

    def _not_implemented_response(self):
        """
        Generate a default not implemented response for process management operations.

        Returns:
            ProcessInstanceList indicating the operation is not implemented.
        """
        return ProcessInstanceList(
            name=self.name,
            token=None,
            values=[],
            flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
        )

    def _create_broadcast_service(self, name, session):
        self.broadcast_service = None

    def _boot_impl(self, boot_request: BootRequest) -> ProcessInstanceList:
        """
        Returns default not implemented response to indicate communication is working
        """
        return self._not_implemented_response()

    def _terminate_impl(self) -> ProcessInstanceList:
        """
        Returns default not implemented response to indicate communication is working
        """
        return self._not_implemented_response()

    def _restart_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        """
        Returns default not implemented response to indicate communication is working
        """
        return self._not_implemented_response()

    def _kill_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        """
        Returns default not implemented response to indicate communication is working
        """
        return self._not_implemented_response()

    def _ps_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        """
        Returns default not implemented response to indicate communication is working
        """
        return self._not_implemented_response()

    def _logs_impl(self, log_request: LogRequest) -> LogLines:
        """
        Returns default not implemented response to indicate communication is working
        """
        return LogLines(
            name="process_manager_no_impl",
            token=None,
            uuid=None,
            lines=[],
            flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
        )

    def _flush_impl(self, query: ProcessQuery) -> ProcessInstanceList:
        return self._not_implemented_response()
