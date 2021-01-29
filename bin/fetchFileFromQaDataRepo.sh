#!/bin/bash

set -e
XLRQACACHE="${XDG_CACHE_HOME:-$HOME/.cache}/xcalar_qa_cache"
pathToPlaceholder="$1"
fileName=`basename "$pathToPlaceholder"`
actualFileName="${fileName%.*}"
sha1=`cat "$pathToPlaceholder"`

if ! [ -e "$XLRQACACHE" ]; then
    mkdir -p "$XLRQACACHE"
fi

# See if this file exists in local cache
cacheFilePath="$XLRQACACHE/${actualFileName}/${sha1:0:2}/${sha1:2:2}/${sha1}"
if [ -e "$cacheFilePath" ]; then
    # Do 1 more paranoid check
    shasum=`sha1sum "$cacheFilePath" | cut -d\  -f1`
    if [ "$shasum" != "$sha1" ]; then
        echo >&2 "sha1sum($actualFileName) == $shasum != $sha1 (as specified in $pathToPlaceholder). Nuking $cacheFilePath"
        rm "$cacheFilePath"
    else
        cat "$cacheFilePath"
        exit
    fi
fi

# Cache miss!
cacheFileDir=`dirname "$cacheFilePath"`
if ! [ -e "$cacheFileDir" ]; then
    mkdir -p "$cacheFileDir"
fi

curlTraceFile=$(mktemp /tmp/curl-trace-repo-xcalar.XXXXXXXXXX)
dstSha="`cat ${pathToPlaceholder}`"
dstKey="http://repo.xcalar.net/qa/${actualFileName}/${dstSha:0:2}/${dstSha:2:2}/${dstSha}"
curl --trace-ascii $curlTraceFile -4 --location --retry 20 --retry-delay 3 --retry-max-time 60 "$dstKey" --fail -o "${cacheFilePath}.$$" || { echo >&2 "Failed to download $dstKey"; cat $curlTraceFile >&2; rm $curlTraceFile; exit 1; }
mv ${cacheFilePath}.$$ ${cacheFilePath}
rm $curlTraceFile

cat "$cacheFilePath"
exit

