hostname=$(hostname)
if [[ ! "$hostname" =~ ^np0[0-9]-srv-0[0-9]{2}$ ]]; then
	echo "Error: Integration tests not ran at EHN1, skipping." >&2
	exit 1
fi

dunedaq_integtest_bundle.sh -r "daqsystemtest|drunc" --pytest-options "--process-manager-type ssh-CERN-kafka"
dunedaq_integtest_bundle.sh -r "daqsystemtest|drunc" --pytest-options "--process-manager-type k8s-CERN"
