#!/bin/bash

HERE=${BASH_SOURCE[0]:-${(%):-%x}}
export DRUNC_DIR=$(cd $(dirname ${HERE}) && pwd)
echo "Setting up DRUNC from $DRUNC_DIR"
source $DRUNC_DIR/venv/bin/activate
export DRUNC_DATA=${DRUNC_DIR}/data
echo "DRUNC setup done"
