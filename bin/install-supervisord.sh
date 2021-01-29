#!/bin/bash
#
# 1. Bootstrap pex in a python2.7 env, because pex won't build in py2.6.
# 2. Use the python2.7 pex to build the target but use a specific version
# of python (2.6 in this case). Pex provides a way to specify a custom
# shebang line which we use to let the same pex work on 2.6 and 2.7!
# 3. Profit??
#

# Could make this /usr/bin/python2, but we want to ensure we're using
# 2.6. The lowest common denominator of our supported platforms.

set -e

[ -z "$VIRTUAL_ENV" ] && deactivate 2>/dev/null

PYTHON=${PYTHON:-/usr/bin/python2}
if [ "${PYTHON:0:1}" != / ]; then
    PYTHON_COMMAND=$(command -v $PYTHON)
    SHEBANG="#!/usr/bin/env ${PYTHON}"
else
    PYTHON_COMMAND=$PYTHON
    SHEBANG="#!${PYTHON}"
fi

TMPDIR=/tmp/`id -u`/venv
rm -rf $TMPDIR
mkdir -p $TMPDIR
/usr/bin/virtualenv --python=$PYTHON_COMMAND $TMPDIR
. $TMPDIR/bin/activate

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"

export PATH=$DIR:$PATH
if ! command -v pex.pex >/dev/null; then
    echo >&2 "Building $DIR/pex.pex"
    install-pex.sh $DIR/pex.pex
fi

pexify () {
    pex.pex --python=${PYTHON} --python-shebang="${SHEBANG}" -v "$@"
}

for prog in supervisorctl supervisord echo_supervisord_conf; do
    pexify supervisor -c ${prog} -o ${prog}.pex
done
