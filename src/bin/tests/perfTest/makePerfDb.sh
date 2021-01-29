#!/bin/bash

#
# This script wraps dbUpdate.sql to show how to make a new perfDB
#

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
dbUpdatePath="$DIR/dbUpdate.sql"

if [ $# -lt "1" -o $# -gt "1" ]; then
    echo "Usage: $0 <newDbPath>"
    exit 1
fi

newDbPath="$1"

sqlite3 "$newDbPath" < "$dbUpdatePath"
