#!/bin/bash

filn=$1

ignoreCommand="@SymbolCheckIgnore"

fail()
{
    echo "$1" 1>&2
    exit 1
}

declare -A bannedSyms
for sympair in `cat $XLRDIR/src/data/banned.symbols | egrep -v ^#`
do
    sym=`echo $sympair | cut -f1 -d':'`
    replacement=`echo $sympair | cut -f2- -d':'`
    bannedSyms[$sym]="$replacement"
done

for sym in "${!bannedSyms[@]}"
do
    replacement="${bannedSyms[$sym]}"

    egrep -q "$sym[ \t]*\(" $filn
    if [ "$?" = "0" ]
    then
        egrep "$sym[ \t]*\(" $filn | egrep -q -v "[A-Za-z0-9_]$sym[ \t]*\("
        if [ "$?" = "0" ]
        then
            egrep "$sym[ \t]*\(" $filn | grep -q -v '^[ \t]*//'
            if [ "$?" = "0" ]
            then
                egrep -B 2 "$sym[ \t]*\(" $filn | \
                        head -n2 | \
                        grep -q $ignoreCommand
                if [ "$?" != "0" ]
                then
                    fail "$filn: ($sym() is unsafe.  use $replacement() instead)"
                fi
            fi
        fi
    fi
done

exit 0
