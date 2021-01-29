#!/bin/bash

#
# Xcalar internal use only
#
# Xcalar Support debugging facility
#
# Simultaneously load ASUP usrnode cores into multiple gdb tmux panes

launchTmuxDbg() {
    local dbgPath=$1
    local targetPane=$2
    osFile="$dbgPath/config/os-release"
    if [ ! -f "$osFile" ]
    then
        echo "Cannot find $osFile, check ASUP path..."
        exit 1
    fi

    declare -A containerMap=(["CentOS-6"]="el6" ["CentOS-7"]="el7")
    # XXX: Dunno if this always exists and what its format is for Ubuntu
    source "$osFile"
    coreFiles=$(ls $dbgPath/coreDumps/core.usrnode.*)
    usrNode="$dbgPath/bin/usrnode"

    container="${containerMap[$CENTOS_MANTISBT_PROJECT]}"
    if [ ! "$container" ]
    then
        echo "$CENTOS_MANTISBT_PROJECT not supported"
        exit 1
    fi

    for cf in $coreFiles
    do
        echo "Creating tmux pane for core: $cf"
        tmux split-window -dt "$targetPane" "crunpwd $container gdb -ex 'set pagination off' -ex bt $usrNode $cf"
    done;
    echo "Use <meta-arrow> to navigate amongst panes (e.g. <ctrl-b down>)"
    echo "Use <meta-z> to zoom pane in/out (e.g. <ctrl-b z>)"

    tmux select-layout -t "$targetPane" even-vertical
    if [ "$targetPane" == "$TMUX_PANE" ]
    then
        tmux select-pane -t 1
    fi
}

usage()
{
    echo "Usage:" 1>&2
    echo "        xcasupdbg.sh -f <asupId> [-o <asupDownloadPath>] # Download ASUP and load into gdb tmux panes" 1>&2
    echo "        xcasupdbg.sh -o <asupRootPath> # Load existing ASUP into gdb tmux panes" 1>&2
    echo "        xcasupdbg.sh -h" 1>&2
    exit 1
}

ASUPPATH=""
DOASUP=false

while getopts "f:o:h" opt; do
  case $opt in
      f) DOASUP=true && ASUPID=$OPTARG;;
      o) ASUPDLOPT="-o $OPTARG" && ASUPPATH="$OPTARG";;
      *) usage;;
  esac
done

if [ ! "$TMUX" ]
then
    echo "Requires running in tmux session. Try:"
    echo "    tmux new"
    echo "then rerun this command"
    exit 1
fi

# Save the current pane before any long ops; if we're combining gdb with fetch, the
# fetch can take a long time and the user has probably switched to other windows
# that we don't want to split.
targetPane="$TMUX_PANE"

if $DOASUP
then
    ASUPPATH=$(xcsupport.sh -f $ASUPID $ASUPDLOPT 2>&1 |tee /dev/tty | tail -n1 |cut -d' ' -f5)
fi

if [ ! -d "$ASUPPATH" ]
then
    echo "Invalid ASUP path: $ASUPPATH"
    exit 1
fi

pushd "$ASUPPATH" > /dev/null
launchTmuxDbg "$ASUPPATH" "$targetPane"
popd > /dev/null
