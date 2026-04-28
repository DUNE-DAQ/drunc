"""
Process metadata management for remote processes.
"""

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class ProcessMetadata:
    """
    Metadata about a remote process.

    Stores process information that needs to persist across connections,
    including the remote process ID for signal delivery.
    """

    pid: Optional[int] = None
    hostname: Optional[str] = None
    user: Optional[str] = None
    started_at: Optional[float] = None
    tree_id: Optional[str] = None
    role: Optional[str] = None
    name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary for JSON serialisation."""
        return {
            "pid": self.pid,
            "hostname": self.hostname,
            "user": self.user,
            "started_at": self.started_at,
            "tree_id": self.tree_id,
            "role": self.role,
            "name": self.name,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProcessMetadata":
        """
        Create ProcessMetadata from dictionary.

        Args:
            data: Dictionary containing metadata fields

        Returns:
            ProcessMetadata instance
        """
        return cls(
            pid=data.get("pid"),
            hostname=data.get("hostname"),
            user=data.get("user"),
            started_at=data.get("started_at"),
            tree_id=data.get("tree_id"),
            role=data.get("role"),
            name=data.get("name"),
        )

    def to_json(self) -> str:
        """
        Serialise metadata to JSON string.

        Returns:
            JSON string representation
        """
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_json(cls, json_str: str) -> "ProcessMetadata":
        """
        Deserialise metadata from JSON string.

        Args:
            json_str: JSON string containing metadata

        Returns:
            ProcessMetadata instance
        """
        return cls.from_dict(json.loads(json_str))

    @staticmethod
    def compute_role_from_tree_id(tree_id: str, is_controller: bool = False) -> str:
        """
        Determines the role of a process based on its tree_id and executable type.

        Mirrors the role mapping used by the K8s process manager:
            - empty tree_id                             -> "unknown"
            - tree_id == "0" + is_controller            -> "root-controller"
            - tree_id == "1"                            -> "local-connection-server"
            - tree_id starts with "0." + is_controller  -> "segment-controller"
            - tree_id starts with "0." + not controller -> "application" (any depth)
            - otherwise                                 -> "infrastructure-applications"

        Args:
            tree_id: Dot-separated hierarchical identifier (e.g. "0", "0.1", "0.1.2.3").
            is_controller: True if the process executable is a drunc-controller.

        Returns:
            Role string: "root-controller",
                        "segment-controller", "application",
                        "infrastructure-applications", or "unknown"
        """
        if not tree_id:
            return "unknown"
        elif is_controller:
            if tree_id == "0":
                return "root-controller"
            elif tree_id.startswith("0."):
                return "segment-controller"
            else:
                return "infrastructure-applications"
        else:
            if tree_id.startswith("0."):
                return "application"
            else:
                return "infrastructure-applications"
