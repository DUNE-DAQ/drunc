"""
Tests for ProcessMetadata.compute_role_from_tree_id role classification logic.
"""

import pytest

from drunc.processes.process_metadata import ProcessMetadata


class TestComputeRoleFromTreeId:
    """Test role classification based on tree_id and is_controller."""

    @pytest.mark.parametrize(
        "tree_id, is_controller, expected_role",
        [
            # Root roles
            ("0", True, "root-controller"),
            ("0", False, "infrastructure-applications"),
            # Segment-controller (requires is_controller=True)
            ("0.1", True, "segment-controller"),
            ("0.1.2", True, "segment-controller"),
            ("0.1.2.3", True, "segment-controller"),
            # Application at various depths (requires 0. prefix, is_controller=False)
            ("0.1", False, "application"),
            ("0.1.2", False, "application"),
            ("0.1.2.3", False, "application"),
            ("0.2.3.4", False, "application"),
            # Local connection server
            ("1", False, "infrastructure-applications"),
            ("1", True, "infrastructure-applications"),
            # Other non-0-prefixed (infrastructure)
            ("infra.process", False, "infrastructure-applications"),
            ("infra.process", True, "infrastructure-applications"),
            ("2.3", False, "infrastructure-applications"),
            ("2.3", True, "infrastructure-applications"),
            # Empty
            ("", False, "unknown"),
            ("", True, "unknown"),
        ],
    )
    def test_compute_role_from_tree_id(
        self, tree_id: str, is_controller: bool, expected_role: str
    ):
        """Verify role classification matches expected value."""
        result = ProcessMetadata.compute_role_from_tree_id(
            tree_id, is_controller=is_controller
        )
        assert result == expected_role, (
            f"compute_role_from_tree_id({tree_id!r}, is_controller={is_controller}) "
            f"returned {result!r}, expected {expected_role!r}"
        )
