#!/bin/bash

find src -executable -type f | egrep -v '\.(sh|py|csv|exp|json|js)$' | egrep -v '^src/data/' | xargs -r ldd | egrep 'lib(thrift|hdfs3)'
if [ $? -ne 0 ]; then
    exit 0
fi
exit 1
