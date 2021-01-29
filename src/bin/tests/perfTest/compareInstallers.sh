#!/bin/bash

#
# This script compares the performance of two different xcalar packages
#

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

XcalarRoot="$DIR/../../../.."

if [ $# -lt "2" -o $# -gt "2" ]; then
    echo "Usage: $0 <installer1> <installer2>"
    exit 1
fi

installer1="$1"
installer2="$2"

# This is local to wherever the user is running this
tmpDb="comparePerf-$RANDOM.db"

while [ -e "$tmpDb" ]; do
    tmpDb="comparePerf-$RANDOM.db"
done

set -e
echo "Making temp SQLite db '$tmpDb'..."
"$DIR/makePerfDb.sh" $tmpDb

echo "Testing installer '$installer1'"
python "$DIR/runPerf.py" -n 1 -r "$tmpDb" --remote -p "$installer1" -s "$installer1"
echo "Testing installer '$installer2'"
python "$DIR/runPerf.py" -n 1 -r "$tmpDb" --remote -p "$installer2" -s "$installer2"

echo "Analyzing '$installer2' vs. '$installer1'"
python "$DIR/perfResults.py" -r $tmpDb
ret="$?"

if [ "$ret" = "0" ]; then
    echo "No Regression"
else
    echo "Performance Regression"
fi

echo "Raw results left around in '$tmpDb'"
echo "Try running $ sqlite3 $tmpDb \"SELECT * FROM commands\""
set +e
