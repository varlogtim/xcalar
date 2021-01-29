#!/bin/bash

# Run the functional tests on a GCE cluster (XXX or a local cluster, later).
# Timestamp the run of each test and copy logs to a directory
# on freenas (/freenas/qa/FuncTestLogs/yyyy-mm-dd-gitSha/...).
set -x


getPids()
{
	pids="$(gcloud compute ssh $CLUSTER"-1" --command "pgrep 'usrnode|childnode'")"
}

if [ -z "$1" ] || [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    echo "usage: $0 <installer-url> <node count (default: 4)> <cluster (default: `whoami`-xcalar)>"
    exit 1
fi

# We may set XLRINFRA externally when integrating this into jenkins.
# If it's not already set, assume it is in the default location for the
# local user.
XLRINFRA="${XLRINFRA:-$HOME/xcalar-infra}"
if [ ! -d $XLRINFRA ]; then
    echo "Can't find xcalar-infra directory!"
    exit 1
fi

# Default node count is 4
COUNT="${2:-4}"

# Default cluster name
CLUSTER="${3:-`whoami`-xcalar}"

# Set environment variables for use by the gce-cluster script
NOTPREEMPTIBLE=1
export NOTPREEMPTIBLE
# Need enough space for potentially multiple cores.
DISK_SIZE=80GB
export DISK_SIZE
SCRIPT_DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
# Custom settings for functional tests come from a template config file.
# Some adjust run time and test complexity, some are required to run the
# functional tests at all.
CONFIG_TEMPLATE=$SCRIPT_DIR/shortRunTemplate.cfg
export CONFIG_TEMPLATE

# Create the cluster instances in GCE
ret=`$XLRINFRA/gce/gce-cluster.sh $1 $COUNT $CLUSTER`

until $XLRINFRA/gce/gce-cluster-health.sh $COUNT $CLUSTER; do
  echo "Waiting for $CLUSTER with $COUNT instances to come up..."
  sleep 3
  # 10 minutes of retries
  try=$(( $try + 1 ))
  if [[ $try -gt 200 ]]; then
  	$XLRINFRA/gce/gce-cluster-delete.sh $CLUSTER || true
  	exit 1
  fi
done

LOGDIRROOT=/freenas/qa/FuncTestLogs

# For now, use the current date for the datestamp in the directory name.
# XXX Might want to change this to the date of the build, as provided in
# the name of the installer, if available.
DATESTAMP=`date "+%Y-%m-%d"`

# Get the SHA of the running usrnode directly via xccli
# XXX next line is for running locally
#versOut="`$XLRDIR/src/bin/cli/xccli -c "version"`"
versOut="`gcloud compute ssh $CLUSTER"-1" --command "/opt/xcalar/bin/xccli -c \"version\""`"
# Extract the SHA from the backend version string
SHA=(`echo $versOut | awk 'BEGIN {FS="-"}{print $46}' | awk 'BEGIN {FS=" "}{print $1}'`)
if [ "$SHA" == "" ]; then
    echo "Invalid version string from xccli -- exiting"
    exit 1
fi

LOGDIR=$LOGDIRROOT/$DATESTAMP-$SHA
if [ ! -d $LOGDIR ]; then
    mkdir -p $LOGDIR
fi

TIMESTAMP=`date "+%H%M%S"`
LOGFILE=$LOGDIR/T$TIMESTAMP.log

IFS=$'\n'
ii=0
XCCLI="$XLRDIR/src/bin/cli/xccli"

# start usrnode
# XXX For local testing
#`$XLRDIR/src/bin/usrnode/launcher.sh 1`

# XXX wait for usrnode to start
#sleep 10

SUCCESS_CNT=0
FAIL_CNT=0
touch $LOGFILE
echo `date "+%Y-%m-%dT%H:%M:%S"` "Running Functional Tests on $CLUSTER, consisting of $COUNT nodes" >> $LOGFILE 2>%1
echo "---------------------------------------------------------------" >> $LOGFILE 2>&1
for test in `gcloud compute ssh $CLUSTER"-1" --command "/opt/xcalar/bin/xccli -c \"functests list\""`
do
    # Skip the first line from the list, which is just the number of tests
    if [ "$ii" -ge 1 ]; then

	# XXX Skip tests that are currently getting stuck or run forever
	if [[ $test == *"libdatasourceptr"* ]]; then
	    continue
	fi

	# start usrnodes
#	`$XLRDIR/src/bin/usrnode/launcher.sh 1` >> $LOGFILE 2>&1
	$XLRINFRA/gce/gce-cluster-restart.sh $COUNT $CLUSTER

	# Wait until we see a valid version string from xccli.  Then we know that
	# the usrnodes on all nodes are up and ready.
	vers=""
	try=0
	while [ "$vers" == "" ]
	do
	    sleep 2
	    versOut="`gcloud compute ssh $CLUSTER"-1" --command "/opt/xcalar/bin/xccli -c \"version\""`"
	    vers=`echo $versOut | awk 'BEGIN {FS="-"}{print $46}'`
	    try=$(( $try + 1 ))
	    if [[ $try -gt 200 ]]; then
		echo "Can't get version from usrnodes -- restart failed.  Continuing." >> $LOGFILE 2>%1
		break
	    fi
	done

	# run and time the test
	echo `date "+%Y-%m-%dT%H:%M:%S"` "Running funcTest $test" >> $LOGFILE 2>&1
	SECONDS=0
#	$XCCLI -c "functests run --testCase $test" >> $LOGFILE 2>&1
	STATUS="$(gcloud compute ssh $CLUSTER"-1" --command "/opt/xcalar/bin/xccli -c \"functests run --testCase $test\"" | tee -a $LOGFILE | grep Success)"
	if [ "$STATUS" == "" ]; then
	    echo `date "+%Y-%m-%dT%H:%M:%S"` "FuncTest $test FAILED after `date -u -d @${SECONDS} +"%T"`" >> $LOGFILE 2>&1
	    FAIL_CNT=$(( $FAIL_CNT + 1 ))
	else
	    echo `date "+%Y-%m-%dT%H:%M:%S"` "Successfully completed funcTest $test in `date -u -d @${SECONDS} +"%T"`" >> $LOGFILE 2>&1
	    SUCCESS_CNT=$(( $SUCCESS_CNT + 1 ))
	fi

	# shutdown usrnodes gracefully
#	$XLRDIR/src/bin/cli/xccli -c "shutdown" >> $LOGFILE 2>&1
	gcloud compute ssh $CLUSTER"-1" --command "/opt/xcalar/bin/xccli -c \"shutdown\"" >> $LOGFILE 2>&1
	echo "--------" >> $LOGFILE 2>&1

	# Allow five minutes for a potential core dump to complete
        for i in `seq 1 600`;
        do
            sleep 1
            getPids
            if [ -z "$pids" ]; then
                break
	    elif [ $i -eq 600 ]; then
		echo "Failed to shut down Xcalar cleanly (pids: $pids)" >> $LOGFILE 2>&1
		echo "Restarting Xcalar forcefully" >> $LOGFILE 2>&1
            fi
        done

	sleep 5
        # Early termination for debug
#	if [ "$ii" -ge 2 ]; then
#	    break
#	fi
    fi
    ii=$(( $ii + 1 ))
done
	echo "-----------------------" >> $LOGFILE 2>&1
	echo "FuncTest run completed." >> $LOGFILE 2>&1
	echo "Tests succeeded: $SUCCESS_CNT" >> $LOGFILE 2>&1
	echo "Tests failed: $FAIL_CNT" >> $LOGFILE 2>&1

# copy Xcalar.log files from all nodes
for jj in `seq 1 $COUNT`
do
    node=$(( $jj - 1 ))
    gcloud compute copy-files $CLUSTER"-"$jj:/var/log/Xcalar.log $LOGDIR/T$TIMESTAMP.node$node.Xcalar.log
done

# XXX TODO Look for cores, tgz them locally, then copy tgz to directory.
# We should give core files a name that includes a core creation datetime stamp,
# so we can line them up with the logs to determine which test caused the core
# dump.  For now, we just list them.
# XXX We may want to copy cores in the test loop above instead, to avoid running
# out of space in the GCE instance's root dir.
for jj in `seq 1 $COUNT`
do
    node=$(( $jj - 1 ))
    echo "----------------"
    echo "Cores on node $node:"
    gcloud compute ssh $CLUSTER"-"$jj --command "ls -al / | grep core"  >> $LOGFILE 2>&1
done


# XXX Keep GCE instances around for now for debug.  Please be careful as this
# can get expensive!
LEAVE_ON_FAILURE=true

# delete GCE instances when done
if [ $FAIL_CNT -eq 0 ]; then
	$XLRINFRA/gce/gce-cluster-delete.sh $CLUSTER || true
else
    echo "One or more tests failed"
    if [ "$LEAVE_ON_FAILURE" = "true" ]; then
	echo "As requested, cluster will not be cleaned up."
	echo "Run 'xcalar-infra/gce/gce-cluster-delete.sh ${CLUSTER}' once finished."
    else
	$XLRINFRA/gce/gce-cluster-delete.sh $CLUSTER || true
    fi
fi

if [ $FAIL_CNT -eq 0 ]; then
    exit 0
else
    exit 1
fi
