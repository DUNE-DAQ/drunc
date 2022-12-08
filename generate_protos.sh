#!/bin/bash
for FILE in proto/*.proto;
do
    echo "Generating protocols for ${FILE}"
    python -m grpc_tools.protoc -I./proto --python_out=src/drunc/communication --grpc_python_out=src/drunc/communication ${FILE}
    FILE_NAME=$(basename -- "$FILE")
    CLASS_NAME="${FILE_NAME%.*}"
    PY_OUT_FILE_NAME="src/drunc/communication/${CLASS_NAME}_pb2_grpc.py"
    # echo "FILE_NAME        " $FILE_NAME
    # echo "CLASS_NAME       " $CLASS_NAME
    # echo "PY_OUT_FILE_NAME " $PY_OUT_FILE_NAME
    # echo "s/import ${CLASS_NAME}_pb2/import drunc.communication.${CLASS_NAME}_pb2/"
    sed -i '' "s/import ${CLASS_NAME}_pb2/import drunc.communication.${CLASS_NAME}_pb2/" ${PY_OUT_FILE_NAME}
done


