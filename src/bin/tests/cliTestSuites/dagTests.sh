#!/bin/bash

listDatasetsFromNonZeroNode()
{
    # This test issues a "list datasets" command to a node other than node 0.
    # This tests that our session is recognized when communicating with a
    # non-default usrnode.

    if [ $numNodes -lt 2 ]; then
        echo "Warning: cannot run listDatasetsFromNonZeroNode with only one usrnode. Skipping."
        return 0
    fi

    # Create a new session to use on node 1.  A session can only be active on
    # one node at a time.

    n1SessionNewOrReplace "" "$cliTestSessionNameN1"

    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not create node 1 session \"$cliTestSessionNameN1\""
        return $ret
    fi

    # The api port was set for CLIN1 in cliTest.sh.  Typically it is 18553.
    output=`$CLIN1 --ip localhost -c "list datasets"`
    if [ $? -ne 0 ]; then
        return 1
    fi

    if [[ "$output" =~ "Error" ]]; then
        echo "Error encountered: $output"
        return 1
    fi

    # Clean up after ourselves.  The session must be made inactive before
    # it can be deleted.
    n1SessionInact "" "$cliTestSessionNameN1"
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not force session \"$cliTestSessionNameN1\" inactive"
        return $ret
    fi

    n1SessionDelete "" "$cliTestSessionNameN1"

    return 0
}

# This is a duplicate of the session logic in cliCmds.sh except that it targets
# node 1 instead of node 0.
# XXX The (probably better) alternative is to rewrite all the existing functions
#     to accept the target node as one of the input parameters.
# Attempts to create a session. If session exists, we delete
# the old one and replace with ourselves
n1SessionNewOrReplace()
{
    local userid=$1
    local sessionName=$2

    for ii in `seq 1 2`; do
        n1SessionNew "$userid" "$sessionName"
        local ret=$?
        if [ "$ret" = "0" -o "$ii" != "1" ]; then
            return $ret
        else
            echo "$output" | grep -q "already exists"
            local alreadyExists=$?
            if [ "$alreadyExists" = "0" ]; then
                echo "Trying to delete existing session and re-create"
                n1SessionDelete "$userid" "$sessionName"
                local ret2=$?
                if [ "$ret2" != "0" ]; then
                    printInColor "red" "Failed to delete \"$sessionName\""
                    return $ret2
                fi
            fi
        fi
    done
}

n1SessionNew()
{
    local userid=$1
    local sessionName=$2

    local cmdName="session"
    cmdsTested[$cmdName]=1

    if [ "$userid" = "" ]; then
        local userSuffix=""
    else
        local userSuffix="-u $userid"
    fi

    local sessionNewCmd="$cmdName --new --name \"$sessionName\""
    echo "$sessionNewCmd;" >> "${cliCmdsOutputFile}${userid}"
    output=`$CLIN1 $userSuffix -c "$sessionNewCmd"`
    echo "$output"
    echo "$output" | grep -q "New session created"
    local ret=$?
    return $ret
}

n1SessionInact()
{
    local userid=$1
    local sessionName=$2

    local cmdName="session"
    cmdsTested[$cmdName]=1

    if [ "$userid" = "" ]; then
        local userSuffix=""
    else
        local userSuffix="-u $userid"
    fi

    local sessionInactCmd="$cmdName --inact --name \"$sessionName\""
    echo "$sessionInactCmd;" >> "${cliCmdsOutputFile}${userid}"
    output=`$CLIN1 $userSuffix -c "$sessionInactCmd"`
    echo "$output"
    echo "$output" | grep -q "forced inactive"
    local ret=$?
    return $ret
}

n1SessionDelete()
{
    local userid=$1
    local sessionName=$2

    local cmdName="session"
    cmdsTested[$cmdName]=1

    if [ "$userid" = "" ]; then
        local userSuffix=""
    else
        local userSuffix="-u $userid"
    fi

    local sessionDeleteCmd="$cmdName --delete --name \"$sessionName\""
    echo "$sessionDeleteCmd;" >> "${cliCmdsOutputFile}${userid}"
    output=`$CLIN1 $userSuffix -c "$sessionDeleteCmd"`
    echo "$output"
    echo "$output" | grep -q "deleted"
    local ret=$?
    return $ret
}
