#!/bin/bash

HERE=${BASH_SOURCE[0]:-${(%):-%x}}
export DRUNC_DIR=$(cd $(dirname ${HERE}) && pwd)
echo "Setting up DRUNC from $DRUNC_DIR"
cd $HERE && pip install -e . && cd -
export DRUNC_DATA=${DRUNC_DIR}/data
echo "DRUNC setup done"
