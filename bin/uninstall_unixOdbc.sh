#!/bin/bash

NAME="[$0]:"
DIR=`dirname ${BASH_SOURCE[0]}`

ORIG_DIR=`pwd`

_V=""

function exc () {
    if [[ $_V -eq 1 ]]; then
        "$@"
    else
        "$@" &> /dev/null
    fi
}

function log () {
    echo "$NAME $1"
}

if [ "$1" = "-v" -o "$1" = "--verbose" ]; then
    log "Executing in verbose mode"
    _V="1"
else
    log "Executing in non-verbose mode, run with -v for verbose"
fi

driverFile="/etc/odbcinst.ini"
dsnFile="/etc/odbc.ini"

log "Moving to $DIR"
cd $DIR

log "Keeping unixodbc-bin"
log "To remove run: sudo apt-get remove unixodbc-bin"

log "Removing DSNs"
odbcinst -q -s |
while read -r line;
do
    name=$(echo $line | cut -d [ -f 2 | cut -d ] -f 1)
    echo "Removing $name"
    sudo odbcinst -u -s -l -n $name
done

log "Removing Drivers"
odbcinst -q -d |
while read -r line;
do
    regEx="[^0-9]*([0-9]*).*"
    name=$(echo $line | cut -d [ -f 2 | cut -d ] -f 1)
    echo "Removing $name"
    remOut=$(sudo odbcinst -u -d -n $name)
    [[ $remOut =~ $regEx ]]
    count=${BASH_REMATCH[1]}
    
    for i in `seq 1 $count`;
    do
        sudo odbcinst -u -d -n $name
    done 
done

log "Removing ODBC ini files"
sudo cp /dev/null $driverFile
sudo cp /dev/null $dsnFile

log "Returning to $ORIG_DIR"
cd $ORIG_DIR
