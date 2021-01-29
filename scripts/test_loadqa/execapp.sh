#!/bin/bash

loadapp=$1
queryname=$2
dataflow=$3
params=$4
workbook=$5

echo "xc2 workbook delete $workbook && xc2 workbook run --workbook-file $XLRDIR/$loadapp --query-name $queryname --dataflow-name $dataflow --params $params --sync"
xc2 workbook delete $workbook && xc2 workbook run --workbook-file $XLRDIR/$loadapp --query-name $queryname --dataflow-name $dataflow --params $params --sync
