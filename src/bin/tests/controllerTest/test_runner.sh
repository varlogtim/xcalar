#!/bin/bash

set -ex
echo "Test Runner for Jenkins"
if [[ -z "$XLRDIR" ]]; then
  PREFIX_DIR="."
else
  PREFIX_DIR="${XLRDIR}"
fi

pip3.6 install fastavro --force-reinstall --no-cache-dir --disable-pip-version-check
pip3.6 install colorama --force-reinstall --no-cache-dir --disable-pip-version-check

start_kafka(){
    echo "start kafka ..."
    (cd $XLRDIR/docker/kafka && make up)
}

stop_kafka(){
    echo "Stop kafka ..."
    (cd $XLRDIR/docker/kafka && make down)
}

# Stop any previous lingering instances of kafka
stop_kafka

if ! start_kafka; then
    echo >&2 "Kafka failed to come up"
    pyMarks="$pyMarks and not kafka"
fi

OLD_DIR=$(pwd)

cd ${OLD_DIR}
pytest -vv -rw -x -p no:randomly ${PREFIX_DIR}/src/bin/tests/controllerTest/test_integration_1.py \
  ${PREFIX_DIR}/src/bin/tests/controllerTest/test_sdlc_check_in_out.py \
  ${PREFIX_DIR}/src/bin/tests/controllerTest/test_sdlc_validation.py \
  ${PREFIX_DIR}/src/bin/tests/controllerTest/test_snapshot.py \
  ${PREFIX_DIR}/src/bin/tests/controllerTest/test_state_management.py \
  ${PREFIX_DIR}/src/bin/tests/controllerTest/extensions/test_controller_app_ext.py

stop_kafka
