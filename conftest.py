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
        help="Run gRPC isolation tests",
    )


def pytest_collection_modifyitems(config, items):
    """Skip gRPC tests unless --test-grpc flag is provided"""
    import pytest

    if config.getoption("--test-grpc"):
        # Run all tests when flag is present
        return

    # Skip only tests explicitly marked with @pytest.mark.grpc
    skip_grpc = pytest.mark.skip(reason="Use --test-grpc to run gRPC isolation tests")
    for item in items:
        if item.get_closest_marker("grpc"):
            item.add_marker(skip_grpc)
