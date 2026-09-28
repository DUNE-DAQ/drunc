"""
Process metadata management for remote processes.
"""

import json
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, TypedDict



class ProcessMetadataDict(TypedDict, total=False):
    pid: Optional[int]
    hostname: Optional[str]
    user: Optional[str]
    started_at: Optional[float]
    tree_id: Optional[str]
    role: Optional[str]
    name: Optional[str]


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

    def to_dict(self) -> ProcessMetadataDict:
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
    def from_dict(cls, data: Mapping[str, object]) -> "ProcessMetadata":
        def _get_str(key: str) -> Optional[str]:
            v = data.get(key)
            return v if isinstance(v, str) else None

        def _get_int(key: str) -> Optional[int]:
            v = data.get(key)
            return v if isinstance(v, int) else None

        def _get_float(key: str) -> Optional[float]:
            v = data.get(key)
            if isinstance(v, (float, int)):
                return float(v)
            return None

        return cls(
            pid=_get_int("pid"),
            hostname=_get_str("hostname"),
            user=_get_str("user"),
            started_at=_get_float("started_at"),
            tree_id=_get_str("tree_id"),
            role=_get_str("role"),
            name=_get_str("name"),
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

            - empty tree_id                             -> "unknown"
            - is_controller + tree_id == "0"            -> "root-controller"
            - is_controller + tree_id starts with "0."  -> "segment-controller"
            - is_controller + otherwise                 -> "infrastructure-applications"
            - not controller + tree_id starts with "0." -> "application"
            - not controller + otherwise                -> "infrastructure-applications"

        Args:
            tree_id: Dot-separated hierarchical identifier (e.g. "0", "0.1", "0.1.2.3").
            is_controller: True if the process executable is a drunc-controller.

        Returns:
            Role string: "root-controller", "segment-controller", "application",
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
                return "infrastructure-applications"  # controller outside segment tree
        else:
            if tree_id.startswith("0."):
                return "application"
            else:
                return "infrastructure-applications"
