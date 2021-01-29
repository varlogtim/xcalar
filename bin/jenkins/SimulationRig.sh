#!/bin/bash
set -e
set -x

# Run Customer4 simulation rig against the given host/port.
# Assumes Xcalar server managed elsewhere.

say() {
    echo >&2 "$*"
}

say "Simulation rig START ===="

say "validate inputs ===="
if [ -z $XCALAR_SERVER_HOST ]; then
    say "ERROR: XCALAR_SERVER_HOST cannot be empty"
    exit 1
fi

if [ "$XCALAR_SERVER_HOST" = "localhost" ]; then
    say "ERROR: localhost not supported"
    exit 1
fi

if [ -z $XCALAR_USER ]; then
    say "ERROR: XCALAR_USER cannot be empty"
    exit 1
fi

if [ -z $XCALAR_PASS ]; then
    say "ERROR: XCALAR_PASS cannot be empty"
    exit 1
fi

if [ -z $API_PORT ]; then
    say "ERROR: API_PORT cannot be empty"
    exit 1
fi

if [ -z "$APP_CONFIG_PATH" ]; then
    say "ERROR: APP_CONFIG_PATH cannot be empty"
    exit 1
fi

if [ -z "$RIG_OPTIONS" ]; then
    say "ERROR: RIG_OPTIONS cannot be empty"
    exit 1
fi

say "Xcalar build ===="
cd $XLRDIR
cmBuild clean
cmBuild config prod
cmBuild xce

say "SimulationRig run test_rig_main.py ===="

host_options="-H $XCALAR_SERVER_HOST -a $API_PORT"
user_options="-U $XCALAR_USER -P $XCALAR_PASS"
./src/bin/tests/simulationRig/test_rig_main.py --app-config $APP_CONFIG_PATH $RIG_OPTIONS $host_options $user_options

say "SimulationRig END ===="
