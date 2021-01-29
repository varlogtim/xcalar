#!/bin/bash

set -e

usage() {
    cat << EOF

This script builds and packages the durable data cross-IDL upgrade components.

Usage: $0 [options]
    -o              : Package output directory
    -v              : Verbose mode
    -h              : This help
EOF
}

xLog() {
    if $optVerbose
    then
        echo "$1"
    fi
}

optVerbose=false
optOutDir="$XLRDIR"

while getopts "?hvo:" OPTION
do
    case $OPTION in
        o)
            optOutDir=$OPTARG
            ;;
        v)
            optVerbose=true
            ;;
        h)
            ;&
        ?)
            usage
            exit 0
            ;;
    esac
done

if [ $# -eq 0 ]; then
    usage
    exit
fi

shift $(($OPTIND - 1))
positionalArgs="$@"

packageDir="$optOutDir/upgradeDurables"
mkdir -p "$packageDir"
cp "$XLRDIR/bin/upgradeDurables.sh" "$packageDir"

# Build the current trunk
git checkout trunk
git submodule update --init --recursive
"$XLRDIR/bin/build" prod
trunkSha=$(git rev-parse --short HEAD)
trunkBin="logdump-$trunkSha"
cp "$XLRDIR/src/misc/logdump/logdump" "$packageDir/$trunkBin"
cp "$XLRDIR/src/data/upgradeDurables/"*.cfg "$packageDir"

# Build the branch logdump for preprocessing to the last pre-IDL-repo version
git checkout xcalar-1.2.0-durableConveter
"$XLRDIR/bin/build" prod
branchSha=$(git rev-parse --short HEAD)
branchBin="logdump-$branchSha"
cp "$XLRDIR/src/misc/logdump/logdump" "$packageDir/$branchBin"

cat > "$packageDir/README" <<EOF
Converts durable data to the latest SHA.  Please update
Constants.XcalarRootCompletePath in the two .cfg files as appropriate then run
somehting like the following command:

EOF

echo "unset XLRDIR && ./upgradeDurables.sh -vdc ./$branchBin -p ./$trunkBin" >> "$packageDir/README"

pushd "$packageDir/.." > /dev/null
tar -jcf "$packageDir-$trunkSha-${branchSha}.tar.gz" $(basename "$packageDir")
