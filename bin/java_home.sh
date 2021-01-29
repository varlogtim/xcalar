#!/bin/bash

JAVA_VERSION_REQUIRED=8

java_version() {
    local version
    "$1" -version 2>&1 | head -1 | grep -q '1\.'$JAVA_VERSION_REQUIRED
}

java_home() {
    local jbin
    if ! jbin="$(command -v java)"; then
        return 1
    fi
    jbin="$(readlink -f "$jbin")"
    local jhome
    for jhome in "${jbin%/jre/bin/java}" "${jbin%/bin/java}"; do
        if test -x "$jhome"/bin/javac && java_version "$jbin"; then
            echo "$jhome"
            return 0
        fi
    done
}

if [ -n "$JAVA_HOME" ] && [ -x "${JAVA_HOME}/bin/javac" ]; then
    echo $JAVA_HOME
    exit 0
fi

if JAVA_HOME=$(java_home); then
    echo $JAVA_HOME
    exit 0
fi

if ! test -d "$JAVA_HOME"; then
    echo >&2 "ERROR: JAVA_HOME=$JAVA_HOME required"
elif ! test -x "${JAVA_HOME}/bin/javac"; then
    echo >&2 "$JAVA_HOME/bin/java isn't executable"
elif ! java_version "${JAVA_HOME}/jre/bin/java"; then
    echo >&2 "JAVA $JAVA_VERSION required. Yours is $(java -version 2>&1 | head -1)"
else
    echo >&2 "Not sure what could be wrong. JDK is not working."
fi

exit 1
