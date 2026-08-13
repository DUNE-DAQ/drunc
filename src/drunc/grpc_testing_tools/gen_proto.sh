#!/bin/bash

# Generate gRPC files and fix imports
# Usage: ./generate_grpc.sh test_services.proto

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PROTO_FILE="${1:-test_services.proto}"
BASE_NAME="${PROTO_FILE%.proto}"

# Generate protobuf files
python -m grpc_tools.protoc --python_out=. --grpc_python_out=. --mypy_out=. --mypy_grpc_out=. -I. "$PROTO_FILE"

# Fix import in the gRPC file
GRPC_FILE="${BASE_NAME}_pb2_grpc.py"
if [ -f "$GRPC_FILE" ]; then
    # Add drunc. prefix so it matches your package structure
    sed -i "s/import ${BASE_NAME}_pb2 as/import drunc.grpc_testing_tools.${BASE_NAME}_pb2 as/g" "$GRPC_FILE"
    echo "Generated and fixed $GRPC_FILE"
else
    echo "Warning: $GRPC_FILE not found"
fi