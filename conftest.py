"""
Pytest configuration for the drunc project.
Registers custom command-line options and test markers.
"""


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

    if config.getoption("--test-all"):
        # Run all tests ignoring any optional test markers
        return

    # Skip tests marked with @pytest.mark.grpc
    if not config.getoption("--test-grpc"):
        skip_grpc = pytest.mark.skip(
            reason="Use --test-grpc to run gRPC isolation tests"
        )
        for item in items:
            if item.get_closest_marker("grpc"):
                item.add_marker(skip_grpc)

    # Skip tests marked with @pytest.mark.paramiko
    if not config.getoption("--test-paramiko"):
        skip_paramiko = pytest.mark.skip(
            reason="Use --test-paramiko to run Paramiko tests"
        )
        for item in items:
            if item.get_closest_marker("paramiko"):
                item.add_marker(skip_paramiko)
