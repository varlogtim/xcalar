#!/bin/bash

DIR=`dirname ${BASH_SOURCE[0]}`
testOutputDir="/netstore/users/dwillis/perfOutputs"

listTests()
{
    local dirName=$1
    echo
    echo "Tests for `basename $dirName`:"
    find "$dirName" -maxdepth 1 -exec basename {} \; | cut -d '-' -f 1 | \
        grep "xc" | sort | uniq | cut -d '.' -f 1
    echo
}

if [ $# -lt "1" -o $# -gt "2" ]; then
    echo "Usage: $0 latest"
    echo "Usage: $0 <dateDirectory|'latest'> <testName>"
    echo "Example: $0 /netstore/users/dwillis/perfOutputs/2015-07-27_20\:03/ loadTest"
    exit 1
fi

if [ "$1" = "latest" ]; then
    dateDirectory=`ls -t $testOutputDir | head -1`
    if [ "$dateDirectory" = "" ]; then
        echo "There are no outputs in the default directory '$testOutputDir'"
        exit 1
    fi
    dateDirectory="$testOutputDir/$dateDirectory"
else
    dateDirectory=$1
fi
if [ ! -d "$dateDirectory" ]; then
    echo "'$dateDirectory' does not exist"
    exit 1
else
    dateDirectory=`readlink -f "$dateDirectory"`
fi

# If just 1 arg, print the options
if [ "$#" = "1" ]; then
    listTests "$dateDirectory"
    exit 0
fi

testName=$2
dateName="`basename $dateDirectory`"
maxNumCols="32"

DATATMP=`mktemp /tmp/durationGraph.XXXXX`
rm -rf $DATATMP
touch $DATATMP

oneNodeDir="$dateDirectory/$testName.xc-1"
if [ ! -d $oneNodeDir ]; then
    echo "'$oneNodeDir' does not exist"
    echo "Cannot do a speedup if there is no 1 node comparison"
    listTests "$dateDirectory"
    exit 1
fi

# Add the column names
echo "NumNodes, Speedup" >> $DATATMP

find "$dateDirectory" -name "$testName*" | grep -oP "(\d+)[^\d]*$" | sort -n |
while read numNodes; do
    testCaseDir="$dateDirectory/$testName.xc-$numNodes"
    timeFile="$testCaseDir/time.log"

    sumTime=`cat "$timeFile" | cut -d ',' -f 2 | paste -sd+`
    if [ "$numNodes" = "1" ]; then
        oneNodeData=$sumTime
        speedup="1"
    else
        speedup=`bc -l <<< "$sumTime / $oneNodeData"`
    fi
    echo "$numNodes, $speedup" >> $DATATMP
done

# Execute the plot script

cat $DATATMP

cat <<EOF | gnuplot -persist 2>&1 | grep -v "no valid points"
set title "Duration of $testName ($dateName)"
set xlabel "Number of Nodes"
set ylabel "Speedup vs 1 Node"
plot '$DATATMP' using 1:2 with lines title columnheader
EOF

rm $DATATMP
