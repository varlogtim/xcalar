#!/bin/bash

say() {
    echo >&2 "$*"
}

gitCheckoutIDL() {
    export GERRIT_IDL_REMOTE="ssh://gerrit.int.xcalar.com:29418/xcalar/xcalar-idl.git"
    export GERRIT_IDL_REFS="+refs/heads/*:refs/remotes/origin/*"

    set +e

    git diff-tree --no-commit-id --name-only -r HEAD | grep xcalar-idl
    export XLRIDLCHANGE=$?

    git submodule init
    git submodule sync
    git config -f .gitmodules --get submodule.xcalar-idl.path
    SUBMODULE_HEAD=$(set -o pipefail; git submodule update --init --recursive xcalar-idl | cut -d" " -f 6 | tr -d \')
    export SM_UPDT=$?
    export SUBMODULE_HEAD

    set -e

    if [ $XLRIDLCHANGE -eq 0 ] && [ $SM_UPDT -ne 0 ]; then
        git submodule init

        cd $XLRDIR/xcalar-idl

        git fetch --tags --progress ${GERRIT_IDL_REMOTE} ${GERRIT_IDL_REFS}

        # XXX Get the submodule HEAD from failure message
        set +e
        cd $XLRDIR
        git submodule update --recursive --force xcalar-idl 2> log
        set -e

        # Different git versions produce different errors
        export SUBMODULE_HEAD=$(cat log | grep "reference is not a tree" | cut -d" " -f7)
        if [ -z $SUBMODULE_HEAD ]; then
            export SUBMODULE_HEAD=$(cat log | grep "unadvertised object" | cut -d" " -f10)
        fi

        cd $XLRDIR/xcalar-idl

        git ls-remote | grep changes > git-ls-remote

        found=0
        while read line; do
            IDL_REVIEW_HEAD=$(echo $line | xargs | cut -d" " -f1)
            IDL_REVIEW_BRANCH=$(echo $line | xargs | cut -d" " -f2)

            if [ "$IDL_REVIEW_HEAD" = "$SUBMODULE_HEAD" ]; then
                echo $IDL_REVIEW_HEAD, echo $IDL_REVIEW_BRANCH
                git fetch --tags --progress ${GERRIT_IDL_REMOTE} $IDL_REVIEW_BRANCH
                git checkout -f $IDL_REVIEW_HEAD
                found=1
                break
            fi
        done < git-ls-remote

        rm git-ls-remote

        if [ $found -eq 0 ]; then
            say "$SUBMODULE_HEAD was not found in any of the xcalar-idl reviews on gerrit."
            say "Did you push xcalar-idl change for gerrit review?"
            exit 1
        fi
    fi

    cd $XLRDIR
}

genBuildArtifacts() {

    set +e # make best effort
    set -x # show your work

    # If first arguement is True, we will only archive logs and stats.
    # Defaults to collect everything.
    onlyLogsAndStats=${1:-false}

    NETLOGS=${NETSTORE}/${JOB_NAME}/${BUILD_ID}
    mkdir -p $NETLOGS
    mkdir -p $XLRDIR/tmpdir

    if [ "$PYTESTINTERROR" = 1 ] ; then
        if [ -f $XLRDIR/buildOut/.pytest_coverage ]; then
            cp $XLRDIR/buildOut/.pytest_coverage $NETLOGS
        fi
    fi

    if [ "$onlyLogsAndStats" = false ] ; then
        # Find core files, dump backtrace & tar core files into core.tar as
        # it'll get gzipped later.
        gdbcore.sh -c core.tar $XLRDIR /var/log/xcalar /var/tmp/xcalar-root 2> /dev/null
    fi
    if [ -f core.tar ]; then
        corefound=1
        mv core.tar $XLRDIR/tmpdir
    else
        corefound=0
    fi

    # Copy files in /tmp after START_TIME to tmpdir/ - only these files will be archived (not entire /tmp dir)
    find /tmp ! -path /tmp -newer /tmp/${JOB_NAME}_${BUILD_ID}_START_TIME 2> /dev/null | xargs cp --parents -rt $XLRDIR/tmpdir/

    # make sure we actually have pigz, so we get logs on all systems
    local zprog
    zprog=$(command -v pigz) || zprog=$(command -v gzip)
    taropts="--warning=no-file-changed --warning=no-file-ignored --use-compress-prog=$zprog"
    PIDS=()
    if [ "$onlyLogsAndStats" = true ]; then
        dirList=(/var/log/xcalar /var/opt/xcalar/DataflowStatsHistory)
    else
        dirList=(tmpdir /var/log/xcalar $(dirname $CADDYLOG 2>/dev/null ||
            true) /var/opt/xcalar/dataflows
        /var/opt/xcalar/DataflowStatsHistory /var/opt/xcalar/oomBcDebugTraces)
    fi

    for dir in ${dirList[*]}; do
        tarfile="${dir#/}"
        tarfile="${tarfile//\//_}.tar.gz"
        tar -cf "${NETLOGS}/$tarfile" $taropts ${dir} >/dev/null 2>&1 &
        PIDS+=($!)
    done

    # If the dir 'oomBcDebugTraces' had failed to be created, but leak traces
    # had to be dumped, they would've been attempted to be dumped into the
    # root dir instead. Check for their existence, and tar them up if found.
    # NOTE: there may be an empty oomBcDebugTraces tar file created if the
    # config param for it (CtxTracesMode) was 0 when the job ran.

    LEAK_TRACES="/var/opt/xcalar/bcleak-traces-*.json"
    if ls $LEAK_TRACES 1> /dev/null 2>&1; then
        leak_trace_files_exist=true
        # strip leading '/' prefix in tarfile name
        tarfile="${LEAK_TRACES#/}"
        # replace '/' with '_'
        tarfile="${tarfile//\//_}.tar.gz"
        # strip the '-*.json' suffix from tarfile name
        tarfile="${tarfile//-\*\.json/}"
        tar -cf "${NETLOGS}/$tarfile" ${LEAK_TRACES} > /dev/null 2>&1 &
        PIDS+=($!)
    fi

    # If there are bc leak trace files in root dir or in a sub-dir, flag this
    # so that the job can exit with a non-zero (failure) exit code
    if [ "$leak_trace_files_exist" = true ] ||
       [ -d /var/opt/xcalar/oomBcDebugTraces ]; then
        bcLeaksExist=1
    else
        bcLeaksExist=0
    fi

    wait "${PIDS[@]}"
    local ret=$?
    if [ $ret -ne 0 ]; then
        say "tar returned non-zero value"
    fi

    if [ "$onlyLogsAndStats" = false ]; then
        dirList+=( core )
    fi
    for dir in ${dirList[*]}; do
        if [ "$dir" = "/var/log/xcalar" ]; then
            rm $dir/* 2> /dev/null
        else
            if [ -d $dir ]; then
                rm -r $dir/* 2> /dev/null
            fi
        fi
    done

    return $corefound
}

coredump_usrnode() {
    say "Collecting corefile from usrnodes"

    for pid in `pgrep usrnode`; do
        kill -ABRT $pid || true
    done
}

waitForUsrnodesToDie() {
    timeout=800
    counter=0

    while pkill -0 usrnode 2> /dev/null; do
        say "Waiting for usrnode to shutdown"

        if [ $counter -gt $timeout ]; then
            say "usrnode shutdown timed out"
            coredump_usrnode
            return 1
        fi

        sleep 5s
        counter=$(($counter + 5))
    done

    return 0
}

postReviewToGerrit() {
    local ret=$?

    # Review may already have been submitted. In such cases below command will fail.
    set +e
    if [ "$XLRIDLCHANGE" = 0 ]; then
        if [ $ret -eq 0 ]; then
            echo "Verified +1"
            echo "ssh -oStrictHostKeyChecking=no -p 29418 gerrit gerrit review --verified +1 -m '$JOB_NAME passed $BUILD_URL' $SUBMODULE_HEAD"
        else
            echo "Verified -1"
            echo "ssh -oStrictHostKeyChecking=no -p 29418 gerrit gerrit review --verified -1 -m '$JOB_NAME passed $BUILD_URL' $SUBMODULE_HEAD"
        fi
    fi

    genBuildArtifacts

    corefileExists=$?

    say "${JOB_NAME} exited with return code $ret"
    say "genBuildArtifacts returned $corefileExists"
    if [ "$bcLeaksExist" -eq 1 ]; then
        say "Buffer cache leaks exist- debug json files copied to artifacts"
    fi
    say "Build artifacts copied to ${NETSTORE}/${JOB_NAME}/${BUILD_ID}"
    set -e

    ret=$(($ret + $corefileExists + $bcLeaksExist))

    exit $ret
}
