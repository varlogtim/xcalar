#!/bin/bash

TMPDIR=`mktemp /tmp/valgrindCheck.XXXXXX`
rm -rf $TMPDIR
mkdir $TMPDIR

if [ "$2" = "" ]
then
    printf "Usage:\n\tvalgrindCheck <xlrSourceDir> <program>\n" 1>&2
    exit 1
fi

SRCDIR=$1
PROGRAM=`basename $2`

. $SRCDIR/bin/qa.sh

grep ac_cs_config $SRCDIR/config.status | head -1 | fgrep -q 'enable-debug=no'
debug=$?
grep -q `basename $PROGRAM` $SRCDIR/src/data/valgrind.exceptions
ret=$?

# XXX avoid valgrind until we can resolve libhdfs3/protobuf cleanup on exit
# https://groups.google.com/forum/#!topic/protobuf/HB1c7KN2AM4
ret=0
if [ "$ret" = "0" ] || [ "$debug" = "0" ]
then
    # We use stdbuf to force the program to stream; otherwise the output won't
    # be echoed until it has finished entirely
    stdbuf -oL -eL "./$PROGRAM" 2>"$TMPDIR/$PROGRAM.err" |
        tee "$TMPDIR/$PROGRAM.out" |
        grep --line-buffered "#\|1\.\.\|not ok\|ok"
    retval=${PIPESTATUS[0]}
    numTestCases=`grep "1\.\." $TMPDIR/$PROGRAM.out | cut -d . -f 3`
    #emitTap "$TMPDIR/$PROGRAM.out"
    if [ "$debug" = "0" ]
    then
	echo "ok $numTestCases - Test \"Valgrind check\" disabled. Warning: skipping valgrind on $PROGRAM due to production build! # SKIP"
    else
	echo "ok $numTestCases - Test \"Valgrind check\" disabled. Warning: skipping valgrind on $PROGRAM due to $SRCDIR/src/data/valgrind.exceptions! # SKIP"
    fi
    if [ "$retval" != "0" ]
    then
        echo "============================================================" 1>&2
        echo "*** $PROGRAM exited with nonzero return!" 1>&2
        echo "*** $PROGRAM stdout:" 1>&2
        cat $TMPDIR/$PROGRAM.out 1>&2
        echo "*** $PROGRAM stderr:" 1>&2
        cat $TMPDIR/$PROGRAM.err 1>&2
        for filn in `find . -type f -name \*core\*`
        do
            gdb -ex "bt" -ex "quit" $PROGRAM $filn > $TMPDIR/$filn.$PROGRAM.gdb 2>&1
            mv $filn $TMPDIR/$filn.$PROGRAM 
            echo "*** stacktrace:" 1>&2
            tail -20 $TMPDIR/$filn.$PROGRAM.gdb 1>&2
        done

        touch $TMPDIR/fail
    fi
else
    touch $TMPDIR/valgrind.$PROGRAM.log
    stdbuf -oL -eL valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --errors-for-leak-kinds=all --track-fds=yes --track-origins=yes --error-exitcode=1 --trace-children=yes --time-stamp=yes --suppressions=$SRCDIR/src/data/valgrind.false.positives --gen-suppressions=all --log-file=$TMPDIR/valgrind.$PROGRAM.log $PROGRAM 2>$TMPDIR/$PROGRAM.err |
        tee "$TMPDIR/$PROGRAM.out" |
        grep --line-buffered "#\|1\.\.\|not ok\|ok"
    libtoolRetval=${PIPESTATUS[0]}
    numTestCases=`grep "1\.\." $TMPDIR/$PROGRAM.out | cut -d . -f 3`
    #emitTap "$TMPDIR/$PROGRAM.out"
    if [ "$libtoolRetval" = "0" ]
    then
        echo "ok $numTestCases - Test \"Valgrind check\" passed"
    else
        echo "not ok $numTestCases - Test \"Valgrind check\" failed"
        echo "============================================================" 1>&2
        echo "*** VALGRIND CHECK FAILED for $PROGRAM!" 1>&2
        echo "*** $PROGRAM stdout:" 1>&2
        cat $TMPDIR/$PROGRAM.out 1>&2
        echo "*** $PROGRAM stderr:" 1>&2
        cat $TMPDIR/$PROGRAM.err 1>&2
        for filn in `find . -type f -name \*core\*`
        do
            gdb -ex "bt" -ex "quit" $PROGRAM $filn > $TMPDIR/$filn.$PROGRAM.gdb 2>&1
            mv $filn $TMPDIR/$filn.$PROGRAM 
            echo "*** stacktrace:" 1>&2
            tail -20 $TMPDIR/$filn.$PROGRAM.gdb 1>&2
        done

        touch $TMPDIR/fail
    fi
fi

retval=0
if [ -e "$TMPDIR/fail" ]
then
    retval=1
    echo "*** Leaving $TMPDIR available for your inspection" 1>&2
    echo "============================================================" 1>&2
else
    if [ `dirname $TMPDIR` = "/tmp" ]; then
        echo "Deleting $TMPDIR"
        rm -rf $TMPDIR
    fi
fi

exit $retval
