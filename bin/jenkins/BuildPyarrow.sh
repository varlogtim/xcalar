#!/bin/bash

set -e

. bin/xcsetenv
cd pkg/pyarrow

make clean
make pyarrow

pip install twine

cat > pypirc <<'EOF'
[distutils]
index-servers =
  pypixcalar

[pypixcalar]
repository: http://pypi.int.xcalar.com:8080/
username: xcalar
password: xcalar
EOF

twine upload --config-file pypirc --repository pypixcalar --skip-existing dist/pyarrow*
