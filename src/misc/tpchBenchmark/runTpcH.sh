#!/bin/bash
# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

# Script to run TPC-H queries using Xcalar dataflows on a Xcalar cluster, given
# the cluster's node0 IP address supplied in NODE_NAME below
#
# Inputs:
#  - Cluster login: NODE_NAME, SSH_USER (for remote), SSH_PASSWORD (for remote)
#  - Flag indicating remote or local (REMOTE env var: "true" or "false")
#  - Retina files which prep src tables in cluster (LOAD STEP)
#    - DATASET_PATH, DS_DATAFLOWDIR_PATH
#  - Retina files which represent the TPC-H SQL queries (QUERY-EXECUTE STEP)
#    - QUERY_DATAFLOWDIR_PATH
# Process:
#  For each of the two steps (LOAD and QUERY-EXECUTE):
#  - upload retina
#  - run retina
#  - get operation breakdown from retina run
# Outputs:
#  - Operation breakdown in $RESULTS_DIR/run-x directory, where x is run number
#
# Steps to run:
# -------------
# Set NODE_NAME env-var with IP address or name of node0 of your cluster
#    (e.g. "% export NODE_NAME=lamport.int.xcalar.com" ) 
# Set REMOTE env-var to "true" or "false"
#    ("true" for remote-cluster and "false" for cluster on local stormtrooper)
#    (e.g. '% export REMOTE="false" ')
#
# Typically, esp. for a stormtrooper run, you'll need to set only these two
# env vars, and then running the script will run the TPC-H queries with a
# standard dataset known to work well for stormtroopers.
#
# For a remote cluster, you'd also need to set SSH_USER and SSH_PASSWORD to a
# valid login user/password for the remote cluster. In addition, if you'd like
# to run with a TPC-H dataset stored on that remote cluster, rather than one
# off /netstore, then you'd need to set DATASET_PATH to point to this dataset
# (e.g. "/ssd/tpch/tpch-sf100")
#
# Note that this script needs the accompanying helper scripts, execRetinaMgr.py
# and execRetinaRemote.py to be in the same directory as this script.
# XXX: Eventually these should be packaged, to allow imports, etc.
#
# IF NEEDED: set DATASET_PATH, DS_DATAFLOWDIR_PATH and QUERY_DATAFLOWDIR_PATH.
# See below for what these env-vars mean.
#
# NOTE:
# 1. Script will automatically archive results in a directory called
#    "$RESULTS_DIR". By default, this will be "./results-<prefix_of_node_name>"
# 2. Each run is labelled N+1, where N is the number of last run (N starts as 0)
# 3. In each results sub-directory (e.g. ./results-lamport/run-10), there's a
#    file for each dataflow executed, with the Xcalar version and execution
#    times in the file (per-operation time and total time at end are reported)
# 4. For debugging, change the loglevel from WARN to DEBUG in execRetinaMgr.py

NODE_NAME="${NODE_NAME:-node5.int.xcalar.com}"
REMOTE="${REMOTE:-true}"
SSH_USER="${SSH_USER:-xcalar}"
SSH_PASSWORD="${SSH_PASSWORD:-Welcome1}"

#
# DATASET_PATH is the directory where the 8 TPC-H datasets are stored.
# DS_DATAFLOWDIR_PATH is the directory where the retinas which load the datasets
# (from DATASET_PATH) are stored.  The execution of these dataflows during the
# LOAD step, results in the TPC-H datasets being loaded into 8 different source
# tables in memory.  In the next step, the QUERY EXECUTION step, the script
# executes the query dataflows stored at QUERY_DATAFLOWDIR_PATH - these retinas
# use the source tables created in the LOAD step, to run the TPC-H queries
#

DATASET_PATH="${DATASET_PATH:-/netstore/datasets/tpch_sf5/}"
DS_DATAFLOWDIR_PATH="${DS_DATAFLOWDIR_PATH:-/netstore/qa/dataflows/tpch_dataset_retinas_deftarget}"
QUERY_DATAFLOWDIR_PATH="${QUERY_DATAFLOWDIR_PATH:-/netstore/qa/dataflows/tpch_sql_optimized}"

# Function to execute dataflows and archive results from running the dataflows.
# It uses internal boolean state (BUILD_SRCTABS) to decide which dataflow to
# run: if true, it runs the ones to do the LOAD, and if false, it runs the ones
# to do QUERY EXECUTION.

execRetinas() {
    printf "%s\t%s\t%s\t%s\n" "Query" "LoadTime (s)" "QueryTime (s)" "TotalTime (s)"
    lastrunid=`ls $RESULTS_DIR|sort -n -k1.5|tail -1|sed 's/run-//'`
    if [ -z $lastrunid ]; then
        runid=1
        mkdir -p $RESULTS_DIR/run-$runid
    else
        if [ $BUILD_SRCTABS = "true" ]; then
            runid=`expr $lastrunid + 1`
            mkdir $RESULTS_DIR/run-$runid
        else
            resdir="$RESULTS_DIR/run-$lastrunid"
            query_results_exist=0
            for qf in $resdir/q*; do
                if [[ -e $qf ]]; then
                    query_results_exist=1
                    break
                fi
            done
            if [ $query_results_exist = 1 ]; then
                # this must be a repeat run with same src tables
                # create new results dir otherwise query results for this run
                # may clobber those archived in the previous run
                runid=`expr $lastrunid + 1`
                mkdir $RESULTS_DIR/run-$runid
            else
                runid=$lastrunid
            fi
        fi
    fi
    rundir="$RESULTS_DIR/run-$runid"
    if [ $BUILD_SRCTABS = "true" ]; then
        export DATAFLOWDIR_PATH=$DS_DATAFLOWDIR_PATH
    else
        export DATAFLOWDIR_PATH=$QUERY_DATAFLOWDIR_PATH
    fi
    for retina in $DATAFLOWDIR_PATH/*
    do
        python2.7 execRetinaMgr.py -n ${NODE_NAME} -u ${SSH_USER}    \
            -p ${SSH_PASSWORD} -b $retina -s ${DATASET_PATH}    \
            -g ${BUILD_SRCTABS} -d $rundir -r ${REMOTE}
    done
}

if [ -z ${RESULTS_DIR} ]; then
    NODE_PREFIX=`echo $NODE_NAME | cut -d '.' -f1`
    RESULTS_DIR="results-$NODE_PREFIX"
fi
# create results dir if one does not exist
if [ ! -d "$RESULTS_DIR" ]; then
    mkdir $RESULTS_DIR
fi

# LOAD step
export BUILD_SRCTABS=true
printf "\n\nFirst building the source tables:\n\n"
printf "from dataset path: %s\n" "$DATASET_PATH"
printf "path to dataflows: %s\n\n" "$DS_DATAFLOWDIR_PATH"
execRetinas

# QUERY EXECUTION step
export BUILD_SRCTABS=false
printf "\n\nNow running the TPC-H SQL queries:\n\n"
printf "path to the query dataflows: %s\n" "$QUERY_DATAFLOWDIR_PATH"
execRetinas
