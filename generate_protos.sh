#!/bin/bash

python -m grpc_tools.protoc -I./proto --python_out=src/drunc/communication --grpc_python_out=src/drunc/communication ./proto/command.proto
