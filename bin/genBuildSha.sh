#!/bin/bash
xceBranch="$(git rev-parse --abbrev-ref HEAD)"
xceSha="$(git rev-parse --short=8 HEAD)"
if [ -d "${XLRGUIDIR}" ]; then
    xdBranch="$(cd "${XLRGUIDIR}" && git rev-parse --abbrev-ref HEAD)"
    xdSha="$(cd "${XLRGUIDIR}" && git rev-parse --short=8 HEAD)"
else
    xdBranch='none'
    xdSha="$(git hash-object -t tree /dev/null | cut -c1-8)"
fi

echo "XCE: $xceBranch ($xceSha)"
echo "XD: $xdBranch ($xdSha)"
