#!/bin/bash

source venv/bin/activate
HERE=${BASH_SOURCE[0]:-${(%):-%x}}
export DRUNC_DIR=$(cd $(dirname ${HERE}) && pwd)
export DRUNC_DATA=${DRUNC_DIR}/data
