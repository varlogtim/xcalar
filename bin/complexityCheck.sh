#!/bin/bash

#
# For theoretical background on cyclomatic complexity, see:
# http://en.wikipedia.org/wiki/Cyclomatic_complexity
#

SRCROOT=`dirname \`dirname \\\`which complexityCheck.sh\\\`\``

MAX_COMPLEXITY=12

TMP1=`mktemp /tmp/XXXXXXcomplexity1`
TMP2=`mktemp /tmp/XXXXXXcomplexity2`
TMP3=`mktemp /tmp/XXXXXXcomplexity3`
rm -f $TMP1 $TMP2 $TMP3

for filn in `find . -maxdepth 1 -name \*.c`
do
    pmccabe $filn >> $TMP1
done

cat $TMP1 | while read line
do
    line=`echo "$line" | sed 's/[ \t][ \t]*/ /g' | sed 's/^[ \t][ \t]*//'`
    echo "$line" | cut -f1,6- -d' ' >> $TMP2
done

cat $TMP2 | while read line
do
    complexity=`echo "$line" | cut -f1 -d ' '`
    line=`echo "$line" | cut -f2- -d ' '`
    file=`echo "$line" | cut -f1 -d '('`
    line=`echo "$line" | cut -f2- -d '('`
    linenum=`echo "$line" | cut -f1 -d ')'`
    func=`echo "$line" | cut -f2 -d ' '`
    
    # exempt symlinks to src/3rd
    if [ -L "$file" ] 
    then
	readlink $file | grep -q '/src/3rd/'
	if [ "$?" = "0" ]
	then
	    continue
	fi
    fi

    if [ "$complexity" -gt "$MAX_COMPLEXITY" ]
    then
        file=`basename $file`
        # look for exclusions
        grep -q $file:$func $SRCROOT/src/data/pmccabe.exceptions
        if [ "$?" = "0" ]
        then
            echo "$file:$linenum:$func() has complexity $complexity but is exempted" 1>&2
        else
            echo "============================================================" 1>&2
	    echo "*** CODE COMPLEXITY CHECK FAILED for $file" 1>&2
	    echo "*** $file:$linenum:$func() is too complex($complexity)" 1>&2
	    echo "***     Please simplify/modularize $func()" 1>&2
	    pmccabe -v $file 1>&2
            echo "============================================================" 1>&2
	    touch $TMP3
        fi
    fi
done

RETVAL=0
if [ -e "$TMP3" ]
then
    RETVAL=1
fi

rm -f $TMP1 $TMP2 $TMP3

exit $RETVAL
