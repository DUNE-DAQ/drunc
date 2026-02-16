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
    def compute_role_from_tree_id(tree_id: str) -> str:
        """
        Determines the role of a process based on its tree_id.

        Args:
            tree_id: Hierarchical identifier in format session.segment.application

        Returns:
            Role string: "root-controller", "local-connection-server",
                        "segment-controller", "application", or "unknown"
        """
        if not tree_id:
            return "unknown"
        elif tree_id == "0":
            return "root-controller"
        elif tree_id == "1":
            return "local-connection-server"
        else:
            depth = tree_id.count(".")
            if depth == 1:
                return "segment-controller"
            elif depth == 2:
                return "application"
        return "unknown"
