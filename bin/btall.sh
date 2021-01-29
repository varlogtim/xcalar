#!/bin/bash

# btall is a gdb wrapper that will collect a backtrace from all threads on all
# nodes and print to the screen.  it will filter out uninteresting threads 
# such as idle work queue workers, the message recv thread, etc.

TMPDIR=`mktemp /tmp/btall.XXXXXX`
rm -rf $TMPDIR
mkdir $TMPDIR

DIR=`dirname ${BASH_SOURCE[0]}`
XLRROOT=$DIR/../

displayIfUseful()
{
    btFile=$1
    cat $btFile | tail -n +2 | head -n -1 | sed 's/.* in //' | sed 's/#[0-9][ ]*//' | cut -f1 -d' ' > $TMPDIR/curthread.syms
    for filn in `/bin/ls -1 $XLRROOT/src/data/debug/btall_exceptions`
    do
        diff -q $TMPDIR/curthread.syms $XLRROOT/src/data/debug/btall_exceptions/$filn >/dev/null 2>&1
        if [ "$?" = "0" ]
        then
            return 0
        fi
    done

    cat $btFile
    return 0
}

if [ "$1" = "coredump" ]
then
    pathToCoreDump=$2
    $XLRROOT/bin/gdball.sh coredump $pathToCoreDump "thread apply all bt" > $TMPDIR/btoutput.txt 2>/dev/null
else
    $XLRROOT/bin/gdball.sh "thread apply all bt" > $TMPDIR/btoutput.txt 2>/dev/null
fi

cat $TMPDIR/btoutput.txt | while read curline
do
    echo "$curline" | grep -q 'gdb usrnode'
    if [ "$?" = "0" ]
    then
        inGdbFooter=0
        nodeId=`echo "$curline" | cut -f4 -d' ' | cut -f1 -d')'`
        continue
    fi
    echo "$curline" | grep -q 'A debugging session is active'
    if [ "$?" = "0" ]
    then
        continue
    fi
    echo "$curline" | grep -q 'GNU gdb'
    if [ "$?" = "0" ]
    then
        inGdbHeader=1
        newThread=0
    fi
    echo "$curline" | egrep -q '^Thread'
    if [ "$?" = "0" ]
    then
        inGdbHeader=0
        newThread=1
    fi

    echo "$curline" | egrep -q 'will be detached'
    if [ "$?" = "0" ]
    then
        inGdbFooter=1
    fi

    if [ "$inGdbHeader" = "1" ] || [ "$inGdbFooter" = "1" ]
    then
        continue
    fi

    if [ "$newThread" = "1" ]
    then
        threadId=`echo "$curline" | cut -f2 -d' '`
        if [ -e "$TMPDIR/curthread.txt" ]
        then
            displayIfUseful $TMPDIR/curthread.txt
            rm -f $TMPDIR/curthread.txt
        fi
        echo "Node$nodeId-Thread$threadId:" > $TMPDIR/curthread.txt
        newThread=0
        continue
    fi

    echo "$curline" >> $TMPDIR/curthread.txt
done

if [ -e "$TMPDIR/curthread.txt" ]
then
    displayIfUseful $TMPDIR/curthread.txt
    rm -f $TMPDIR/curthread.txt
fi

rm -rf $TMPDIR
