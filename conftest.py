"""
Pytest configuration for the drunc project.
Registers custom command-line options and test markers.
"""


def pytest_configure(config):
    """Block coverage reporting for integration tests."""
    if any("integtest" in str(arg) for arg in config.args):
        plugin = config.pluginmanager.get_plugin("_cov")
        if plugin:
            plugin.options.no_cov = True


def pytest_addoption(parser):
    """Register custom command-line options for pytest"""
    parser.addoption(
        "--test-grpc",
        action="store_true",
        default=False,
        help="Run gRPC isolation tests for checking gRPC version and settings compatibility",
    )

    parser.addoption(
        "--test-paramiko",
        action="store_true",
        default=False,
        help="Run tests requiring Paramiko for SSH connections",
    )

    parser.addoption(
        "--test-all",
        action="store_true",
        default=False,
        help="Run all tests, including optional ones",
    )


def pytest_collection_modifyitems(config, items):
    """Modify collected test items based on command-line options"""
    import pytest

    def add_skip_marker(marker_name, reason):
        """Apply skip marker to all tests with the specified marker if option not enabled

        Args:
            marker_name: The pytest marker to look for (e.g., 'grpc')
            reason: The skip reason message shown when tests are skipped
        """
        skip_marker = pytest.mark.skip(reason=reason)
        for item in items:
            if item.get_closest_marker(marker_name):
                item.add_marker(skip_marker)

    def skip_if_no_option(marker_name, option_name, reason):
        """
        Helper function to skip tests with a specific marker if the corresponding command-line option is not enabled.

        Args:
            marker_name: The pytest marker to look for (e.g., 'grpc')
            option_name: The command-line option to check (e.g., '--test-grpc')
            reason: The skip reason message shown when tests are skipped
        """
        if not config.getoption(option_name):
            add_skip_marker(marker_name, reason)

    if config.getoption("--test-all"):
        # Run all tests ignoring any optional test markers
        # Note that paramiko tests will only be enabled if --test-paramiko is also specified
        # this is because paramiko is not actively being maintained
        skip_if_no_option(
            "paramiko", "--test-paramiko", "Use --test-paramiko to run Paramiko tests"
        )
        return

    skip_if_no_option(
        "grpc", "--test-grpc", "Use --test-grpc to run gRPC isolation tests"
    )
    skip_if_no_option(
        "paramiko", "--test-paramiko", "Use --test-paramiko to run Paramiko tests"
    )
