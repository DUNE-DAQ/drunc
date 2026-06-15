#!/bin/bash

# Defines a driver script for the drunc integration tests.
# The purpose of these scripts is to run a set of integration test with all of the features of drunc tested, so any introduced changes do not affect functionality of the existing infrastructure.
# Based entirely of the implementation of daqsystemtest_integtest_bundle.sh
# Original author: KAB, 10-Oct-2023

integtest_list=( "process_manager_test.py", "test_failure_mode_death_on_boot_nest_app.py")
let last_test_index=${#integtest_list[@]}-1

usage() {
    declare -r script_name=$(basename "$0")
    echo """
Usage:
"${script_name}" [option(s)]

Options:
    `-h, --help : prints out usage information`
    -f <zero-based index of the first test to be run, default=0>
    -l <zero-based index of the last test to be run, default=${last_test_index}>
    -k <pipe-delimited string to select which tests will be run ('egrep -i' match to test name)>
    -n <number of times to run each individual test, default=1>
    -N <number of times to run the full set of selected tests, default=1>
    --stop-on-failure : causes the script to stop when one of the integtests reports a failure
    --concise-output : suppresses run control and DAQApp messages in order to focus on test results
    --tmpdir : specifies a root directory to use for test output, e.g. a directory instead of '/tmp'
"""
    let counter=0
    echo "List of available tests:"
    for tst in ${integtest_list[@]}; do
        echo "    ${counter}: $tst"
        let counter=${counter}+1
    done
    echo ""
}

# 29-Dec-2025, KAB: Determine if a non-standard pytest tmpdir has been specified
# in the linux shell environment in which this script is being run. We need to know
# this value in order to direct functionality in this script to the right place.
# A user-specified command-line value for the tmpdir over-rides the value determined here.
tmpdir_root=`dst_get_pytest_tmpdir`

# Removes the ANSI characters associated with formatting, including color coding and font styling
CaptureOutputNoANSI() {
    tee -a >(sed -u 's/\x1b\[[0-9;]*m//g' >> "$1")
}
# Captures the output to the specified file, without changing the output
CaptureOutput() {
    tee -a $1
}

GETOPT_TEMP=`getopt -o hs:f:l:k:n:N: --long help,stop-on-failure,concise-output,tmpdir: -- "$@"`
eval set -- "$GETOPT_TEMP"

let first_test_index=0
let individual_test_requested_iterations=1
let full_set_requested_interations=1
let stop_on_failure=0
requested_test_names=
PYTEST_COMMAND="pytest -c /dev/null -s --tb=short"  # our core pytest command, with DAQ printout included and short pytest traceback

while true; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        -f)
            let first_test_index=$2
            shift 2
            ;;
        -l)
            let last_test_index=$2
            shift 2
            ;;
        -k)
            requested_test_names=$2
            shift 2
            ;;
        -n)
            let individual_test_requested_iterations=$2
            shift 2
            ;;
        -N)
            let full_set_requested_interations=$2
            shift 2
            ;;
        --stop-on-failure)
            let stop_on_failure=1
            PYTEST_COMMAND="${PYTEST_COMMAND} -x"  # add the -x option to our pytest command to have it exit on first error
            shift
            ;;
        --concise-output)
            PYTEST_COMMAND="`echo ${PYTEST_COMMAND} | sed 's/ -s//'`"  # remove the -s option to turn off messages from DAQ processes
            shift
            ;;
        --tmpdir)
            tmpdir_root=$2
            export PYTEST_DEBUG_TEMPROOT=${tmpdir_root}
            shift 2
            ;;
        --)
            shift
            break
            ;;
    esac
done

# check if the numad daemon is running
numad_grep_output=`ps -ef | grep numad | grep -v grep`
if [[ "${numad_grep_output}" != "" ]]; then
    echo "*********************************************************************"
    echo "*** DANGER, DANGER, 'numad' appears to be running on this computer!"
    echo "*** 'ps' output:  ${numad_grep_output}"
    echo "*** <ctrl-c> now if you want to abort this testing."
    echo "*********************************************************************"
    sleep 3
fi

# other setup
INITIAL_TIMESTAMP=`date '+%Y%m%d%H%M%S'`
# 30-Dec-2025, KAB: check that the specified tmpdir exists and is writeable
if [[ ! -d ${tmpdir_root} ]]; then
    echo "*** ERROR: directory \"${tmpdir_root}\" does not exist."
    exit 1
fi
if [[ ! -w ${tmpdir_root} ]]; then
    echo "*** ERROR: directory \"${tmpdir_root}\" is not writeable in the current environment."
    exit 1
fi
pytest_user_dir=${tmpdir_root}/pytest-of-${USER}
mkdir -p ${pytest_user_dir}
ITGRUNNER_LOG_FILE="${pytest_user_dir}/drunc_integtest_bundle_${INITIAL_TIMESTAMP}.log"
CURRENT_PID=$$

let number_of_individual_tests=0
let test_index=0
for TEST_NAME in "${integtest_list[@]}"; do
    if [[ ${test_index} -ge ${first_test_index} && ${test_index} -le ${last_test_index} ]]; then
        requested_test=`echo ${TEST_NAME} | egrep -i ${requested_test_names:-${TEST_NAME}}`
        if [[ "${requested_test}" != "" ]]; then
            let number_of_individual_tests=${number_of_individual_tests}+1
        fi
    fi
    let test_index=${test_index}+1
done
let total_number_of_tests=${number_of_individual_tests}*${individual_test_requested_iterations}*${full_set_requested_interations}

# run the tests
let overall_test_index=0  # this is only used for user feedback
let full_set_loop_count=0
while [[ ${full_set_loop_count} -lt ${full_set_requested_interations} ]]; do
    let test_index=0
    for TEST_NAME in "${integtest_list[@]}"; do
        if [[ ${test_index} -ge ${first_test_index} && ${test_index} -le ${last_test_index} ]]; then
            CURRENT_TIMESTAMP=`date '+%Y%m%d%H%M%S'`
            # 15-Dec-2025, KAB: added the export of the following enviromental variable.  This is used
            # by the integrationtest infrastructure to put a bread-crumb file in the directory where
            # the test results are located.  That file, in turn, allows this script to find the directory
            # for the current test, and make a copy of it if the test fails.
            export DUNEDAQ_INTEGTEST_BUNDLE_INFO="${INITIAL_TIMESTAMP};${CURRENT_PID};${CURRENT_TIMESTAMP}"
            requested_test=`echo ${TEST_NAME} | egrep -i ${requested_test_names:-${TEST_NAME}}`
            if [[ "${requested_test}" != "" ]]; then
                let individual_loop_count=0
                while [[ ${individual_loop_count} -lt ${individual_test_requested_iterations} ]]; do
                    let overall_test_index=${overall_test_index}+1
                    echo ""
                    echo -e "\U0001F535 \033[0;34mStarting test ${overall_test_index} of ${total_number_of_tests}...\033[0m \U0001F535" | CaptureOutput ${ITGRUNNER_LOG_FILE}

                    echo -e "\u2B95 \033[0;1mRunning ${TEST_NAME}\033[0m \u2B05" | CaptureOutput ${ITGRUNNER_LOG_FILE}
                    if [[ -e "./${TEST_NAME}" ]]; then
                        ${PYTEST_COMMAND} ./${TEST_NAME} | CaptureOutputNoANSI ${ITGRUNNER_LOG_FILE}
                    elif [[ -e "${DBT_AREA_ROOT}/pythoncode/drunc/integtest/${TEST_NAME}" ]]; then
                        if [[ -w "${DBT_AREA_ROOT}" ]]; then
                            ${PYTEST_COMMAND} ${DBT_AREA_ROOT}/pythoncode/drunc/integtest/${TEST_NAME} | CaptureOutputNoANSI ${ITGRUNNER_LOG_FILE}
                        else
                            ${PYTEST_COMMAND} -p no:cacheprovider ${DBT_AREA_ROOT}/pythoncode/drunc/integtest/${TEST_NAME} | CaptureOutputNoANSI ${ITGRUNNER_LOG_FILE}
                        fi
                    else
                        ${PYTEST_COMMAND} -p no:cacheprovider ${DAQSYSTEMTEST_SHARE}/integtest/${TEST_NAME} | CaptureOutputNoANSI ${ITGRUNNER_LOG_FILE}
                    fi
                    let pytest_return_code=${PIPESTATUS[0]}

                    let individual_loop_count=${individual_loop_count}+1

                    # check if the test failed
                    if [[ ${pytest_return_code} -ne 0 ]]; then
                        # 15-Dec-2025, KAB: make a copy of the pytest directory. This allows
                        # testers to take a look at the results within a reasonable time frame.
                        # (If we can't find the "jq" JSON utility, we simply note that fact
                        # and continue.)
                        # This code makes use of a bread-crumb file that is created by the
                        # integrationtest infrastructure.
                        if [[ "`which jq 2>/dev/null`" != "" ]]; then
                            current_pytest_rundir=""
                            mapfile -t bundle_info_files < <(find "${pytest_user_dir}" -type f -name "bundle_script_info.json" -printf '%T@ %p\n' | grep -v 'failed-' | sort -nr | awk '{print $2}')
                            for info_file in "${bundle_info_files[@]}"; do
                                script_start_time=`jq -r .bundle_script_start_time ${info_file}`
                                script_pid=`jq -r .bundle_script_process_id ${info_file}`
                                individual_test_start_time=`jq -r .individual_test_start_time ${info_file}`
                                if [[ ${script_start_time} -eq ${INITIAL_TIMESTAMP} ]] && \
                                       [[ ${script_pid} -eq ${CURRENT_PID} ]] && \
                                       [[ ${individual_test_start_time} -eq ${CURRENT_TIMESTAMP} ]]; then
                                    current_pytest_rundir=$info_file
                                    break
                                fi
                            done

                            was_successfully_copied=""
                            if [[ "${current_pytest_rundir}" != "" ]]; then
                                pytest_tmpdir=`echo ${current_pytest_rundir} | xargs -r dirname | xargs -r dirname`
                                if [[ "${pytest_tmpdir}" != "" ]]; then
                                    pytest_rootdir=`echo ${pytest_tmpdir} | xargs -r dirname`
                                    pytest_basedir=`echo ${pytest_tmpdir} | xargs -r basename`
                                    if [[ "${pytest_rootdir}" != "" ]] && [[ "${pytest_basedir}" != "" ]]; then
                                        new_dir="${pytest_rootdir}/failed-${pytest_basedir}"
                                        echo ""
                                        echo -e "\U1F535 Copying the files from failed test ${pytest_tmpdir} to ${new_dir}. \U1F535"
                                        cp -pR "${pytest_tmpdir}" "${new_dir}"
                                        if [[ $? == 0 ]]; then
                                            was_successfully_copied="yes"
                                            # 18-Dec-2025, KAB: added the removal of the "current" symbolic links
                                            # from inside the copied directory (since they get broken in the copying)
                                            rm -f "${new_dir}/configcurrent"
                                            rm -f "${new_dir}/runcurrent"
                                        fi
                                    fi
                                fi
                            fi
                            if [[ "${was_successfully_copied}" == "" ]]; then
                                echo ""
                                echo -e "\U1f7e1 WARNING: Unable to copy the pytest directory for this failed test (${current_pytest_rundir}). \U1f7e1"
                            fi
                        else
                            echo ""
                            echo -e "\U1f7e1 WARNING: Unable to find the 'jq' utility which is needed to help identify which pytest directory to copy for this failed test. \U1f7e1"
                        fi

                        # remove stale and surplus directories from failed tests
                        test_dirs_to_remove=()
                        mapfile -t all_failed_test_dirs < <(find ${pytest_user_dir} -maxdepth 1 -type d -printf '%T@ %p\n' | sort -nr | awk '{print $2}' | grep 'failed-')
                        surplus_dirs=("${all_failed_test_dirs[@]:10}")
                        for test_dir in "${surplus_dirs[@]}"; do
                            test_dirs_to_remove+=(${test_dir})
                        done
                        stale_failed_test_dirs=(`find ${pytest_user_dir} -maxdepth 1 -type d -name 'failed-*' -cmin +1560 -print`)
                        for test_dir in "${stale_failed_test_dirs[@]}"; do
                            test_dirs_to_remove+=(${test_dir})
                        done
                        if [[ ${#test_dirs_to_remove[@]} -gt 0 ]];then
                            echo -e "\U1F535 Removing ${#test_dirs_to_remove[@]} old failed test directory(ies). \U1F535"
                            for test_dir in "${test_dirs_to_remove[@]}"; do
                                if [[ -e "${test_dir}" ]]; then
                                    rm -rf "${test_dir}"
                                fi
                            done
                        fi

                        # exit out of this script if the user has requested that we stop on a failure
                        if [[ ${stop_on_failure} -gt 0 ]]; then
                            break 3
                        fi
                    fi
                done
            fi
        fi
        let test_index=${test_index}+1
    done

    let full_set_loop_count=${full_set_loop_count}+1
done

# print out summary information
echo ""                                                   | CaptureOutput ${ITGRUNNER_LOG_FILE}
echo ""                                                   | CaptureOutput ${ITGRUNNER_LOG_FILE}
echo "+++++++++++++++++++++++++++++++++++++++++++++++++"  | CaptureOutput ${ITGRUNNER_LOG_FILE}
echo "++++++++++++++++++++ SUMMARY ++++++++++++++++++++"  | CaptureOutput ${ITGRUNNER_LOG_FILE}
echo "+++++++++++++++++++++++++++++++++++++++++++++++++"  | CaptureOutput ${ITGRUNNER_LOG_FILE}
echo ""                                                   | CaptureOutput ${ITGRUNNER_LOG_FILE}
date                                                      | CaptureOutput ${ITGRUNNER_LOG_FILE}
echo "Log file is: ${ITGRUNNER_LOG_FILE}"                 | CaptureOutput ${ITGRUNNER_LOG_FILE}
echo ""                                                   | CaptureOutput ${ITGRUNNER_LOG_FILE}
summary_string="`egrep $'=====|\u2B95' ${ITGRUNNER_LOG_FILE} | egrep ' in |Running'`"
colorized_summary_string="`echo \"${summary_string}\" | sed 's/passed/passed \\\\U2705/' | sed 's/failed/failed \\\\U274c/' | sed 's/skipped/skipped \\\\U1f7e1/'`"
echo -e "${colorized_summary_string}" | CaptureOutput ${ITGRUNNER_LOG_FILE}

# check again if the numad daemon is running
numad_grep_output=`ps -ef | grep numad | grep -v grep`
if [[ "${numad_grep_output}" != "" ]]; then
    echo ""                                                                                 | CaptureOutput ${ITGRUNNER_LOG_FILE}
    echo "********************************************************************************" | CaptureOutput ${ITGRUNNER_LOG_FILE}
    echo "*** WARNING: 'numad' appears to be running on this computer!"                     | CaptureOutput ${ITGRUNNER_LOG_FILE}
    echo "*** 'ps' output:  ${numad_grep_output}"                                           | CaptureOutput ${ITGRUNNER_LOG_FILE}
    echo "*** This daemon can adversely affect the running of these tests, especially ones" | CaptureOutput ${ITGRUNNER_LOG_FILE}
    echo "*** that are resource intensive in the Readout Apps. This is because numad moves" | CaptureOutput ${ITGRUNNER_LOG_FILE}
    echo "*** processes (threads?) to different cores/numa nodes periodically, and that"    | CaptureOutput ${ITGRUNNER_LOG_FILE}
    echo "*** context switch can disrupt the stable running of the DAQ processes."          | CaptureOutput ${ITGRUNNER_LOG_FILE}
    echo "********************************************************************************" | CaptureOutput ${ITGRUNNER_LOG_FILE}
fi