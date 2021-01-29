#!/bin/bash
#
# Copyright 2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
# Uploads a file to s3://xcasup/uploads/$DATEUNIX/$HOSTNAME/`basename $1`.
# The script will also take care of tar.gz compressing the file before upload.


if [ -r /etc/default/xcalar ]; then
    . /etc/default/xcalar
fi

## Configure these defaults in /etc/default/xcalar
## The default xcasup bucket has write permissions
## for everyone
ASUP_BUCKET_KEY="${ASUP_BUCKET_KEY:-AKIAYEMHKGM3RYLU6TTH}"
ASUP_BUCKET_SECRET="${ASUP_BUCKET_SECRET:-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}"
ASUP_BUCKET="${ASUP_BUCKET:-xcasup}"
ASUP_BUCKET_PREFIX="${ASUP_BUCKET_PREFIX:-uploads/}"
ASUP_REGION="${ASUP_REGION:-us-west-2}"
ASUP_MAXSIZE="${ASUP_MAXSIZE:-5368709120}"

TMPDIR="${TMPDIR:-/var/tmp}"

test -z "$HOSTNAME" && HOSTNAME="$(hostname -f)"
test -z "$HOSTNAME" && HOSTNAME="$(hostname -s)"
test -z "$HOSTNAME" && HOSTNAME="$(hostname)"

set +e

syslog () {
    logger -i -s -t xcasup "$*"
}

get_diskfree () {
    local -i diskFree=0
    diskFree=$(set -o pipefail; df "$1" | tail -n+2 | awk '{print $4*1024}')
    local rc=$?
    echo "$diskFree"
    return $rc
}

get_filesize () {
    local -i fileSize=0
    fileSize="$(stat -L -c '%s' "$1")"
    local rc=$?
    if [ $rc -eq 0 ]; then
        echo "$fileSize"
    fi
    return $rc
}

is_compressed () {
    if command -v file &>/dev/null; then
        file "$1" | grep -q 'gzip compressed data'
    else
        echo "$1" | grep -Eq '\.(tgz|gz)$'
    fi
}

compress () {
    local output="$1" input="$2"
    shift 2

    local -i outputDiskFree= inputFileSize=
    inputFileSize="$(get_filesize "$input")" && \
    outputDiskFree="$(get_diskfree $(dirname "$output"))"
    if [ $? -ne 0 ] || [ $outputDiskFree -lt $inputFileSize ]; then
        # Couldn't determine input file size, or how much free space
        # we have. Bail and use input directly
        echo "$input"
        return 0
    fi

    if echo "$input" | grep -qE '\.tar$'; then
        gzip --to-stdout --fast "$input" > "$output"
    else
        tar Sczf "$output" "$input" >&2
    fi
    local rc=$?
    # If for any reason we failed to compress, then use the input
    # directly
    if [ $rc -eq 0 ]; then
        echo "$output"
    else
        echo "$input"
    fi
    return $rc
}

# Using $1, join the rest of the arguments.
# eg, strjoin , a b c -> a,b,c
strjoin () {
    local IFS="$1"
    shift; echo "$*"
}

hex256() {
  printf "$1" | od -A n -t x1 | sed ':a;N;$!ba;s/[\n ]//g'
}

hmac () {
  printf "$2" | openssl dgst -binary -hex -sha256 -mac HMAC -macopt hexkey:$1 \
              | sed 's/^.* //'
}

# Generate AWS V4 signature
#   $1 AWS Secret Access Key, $2 yyyymmdd $3 Region $4 AWS Service $5 string data to sign
sign() {
  hmac $(hmac $(hmac $(hmac $(hmac $(hex256 "AWS4$1") $2) $3) $4) "aws4_request") "$5"
}

s3put_boto() {
    local fileToUpload="$1"
    local path="$2"

    local python version
    for python in python3.6 python3 python /opt/xcalar/bin/python3.6; do
        if version="$($python -c 'import sys,boto3; sys.stdout.write(boto3.__version__)' 2>/dev/null)"; then
            break
        fi
    done
    if ! [[ $version =~ ^1\. ]]; then
        return 1
    fi

    echo >&2 "Uploading $1 to $2 using $python boto v${version}"
    $python <<PEOF
from __future__ import print_function
import sys
import threading

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._seen_so_far = 0
        self._delta = 0
        self._lock = threading.Lock()
    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            self._delta += bytes_amount
            if self._delta > 100*1024*1024:
                sys.stdout.write(
                    "\\r%s --> %d Mb transferred" % (
                        self._filename, int(self._seen_so_far/(1024*1024))))
                sys.stdout.flush()
                self._delta = 0

s3 = boto3.client('s3', config=Config(signature_version='s3v4'))
GB = 1024 ** 3
config = TransferConfig(multipart_threshold=5 * GB, max_concurrency=10)
bucket = '$ASUP_BUCKET'
obj = '${path#/$ASUP_BUCKET/}'
extraArgs={}
if "$KMS" != 'false':
    extraArgs['ServerSideEncryption'] = 'aws:kms'

s3.upload_file('$fileToUpload', bucket, obj, Config=config, ExtraArgs=extraArgs, Callback=ProgressPercentage('/'.join([bucket, obj])))

PEOF
}

s3put () {
    # Try to use boto3 first
    if s3put_boto "$@"; then
        return
    fi
    local fileToUpload="$1"
    local path="$2"

    local timestamp=$(date -u "+%Y-%m-%d %H:%M:%S")
    local isotime=$(date -ud "${timestamp}" "+%Y%m%dT%H%M%SZ")
    local datescope=$(date -ud "${timestamp}" "+%Y%m%d")
    local s3endpoint="s3-${AWS_REGION}.amazonaws.com"

    local headers=
    local headerList=
    local contentType=
    local curlHeaders=()

    local payloadHash=$(sha256sum $1 | cut -d' ' -f1)

    case "$fileToUpload" in
        *.tar.gz) contentType="application/x-compressed-tar";;
    esac

    if [ -n "$contentType" ]; then
        headers+="content-type:${contentType}\n"
        headerList+="content-type;"
        curlHeaders+=("-H" "Content-Type: ${contentType}")
    fi

    headers+="host:${s3endpoint}\nx-amz-content-sha256:${payloadHash}\nx-amz-date:${isotime}"
    headerList+="host;x-amz-content-sha256;x-amz-date"

    if [ "$KMS" != false ]; then
        headers+="\nx-amz-server-side-encryption:aws:kms"
        headerList+=";x-amz-server-side-encryption"
        curlHeaders+=("-H" "x-amz-server-side-encryption: aws:kms")
    fi

    local canonicalRequest="PUT\n${path}\n\n${headers}\n\n${headerList}\n${payloadHash}"
    local hashedRequest=$(printf "${canonicalRequest}" | sha256sum | cut -d' ' -f1)
    local stringToSign="AWS4-HMAC-SHA256\n${isotime}\n${datescope}/${AWS_REGION}/s3/aws4_request\n${hashedRequest}"
    local signature=$(sign "${AWS_SECRET_ACCESS_KEY}" "${datescope}" "${AWS_REGION}" "s3" "${stringToSign}")
    local authorizationHeader="AWS4-HMAC-SHA256 Credential=${AWS_ACCESS_KEY_ID}/${datescope}/${AWS_REGION}/s3/aws4_request, SignedHeaders=${headerList}, Signature=${signature}"

    curl -s -L -X PUT -T "${fileToUpload}" "https://${s3endpoint}${path}"  \
        -H "Authorization: ${authorizationHeader}" \
        -H "x-amz-content-sha256: ${payloadHash}" \
        -H "Host: ${s3endpoint}" \
        -H "x-amz-date: ${isotime}" "${curlHeaders[@]}"
}

xcasup_upload () {
    local fileInput="$1"
    local path="$2"

    local baseName="$(basename "$fileInput")"
    local fileName="${baseName%%.*}"
    local fileExt="${baseName#*.}"

    LOGFILE="${TMPDIR}/xcasup-$$-${baseName}.txt"
    TMPFILE="${TMPDIR}/xcasup-$$-${baseName}.tar.gz"

    local fileToUpload="$fileInput"
    if ! is_compressed "$fileInput"; then
        # Attempt to compress the file if it isn't already
        tmpDiskFree=$(get_diskfree "$(dirname "$TMPFILE")")
        fileInputSize=$(get_filesize "$fileInput")
        if [ $tmpDiskFree -lt $fileInputSize ]; then
            syslog "WARNING: Not enough disk space to compress $fileInput to $TMPFILE ($fileInputSize > $tmpDiskFree)"
        elif ! fileToUpload="$(compress "$TMPFILE" "$fileInput")"; then
            syslog "WARNING: Failed to compress $fileInput to $TMPFILE"
            fileToUpload="$fileInput"
        fi
    fi

    if [ "$fileToUpload" != "$fileInput" ]; then
        syslog "Using $fileToUpload instead of $fileInput"
    fi


    if fileSize="$(get_filesize "$fileToUpload")" && [ "$fileSize" -gt "$ASUP_MAXSIZE" ]; then
        syslog "WARNING: $fileToUpload exceeds maximum size ($fileSize > $ASUP_MAXSIZE). Attempting to upload anyway."
    fi

    fullpath="$(readlink -f "$fileToUpload")"
    resource="/${ASUP_BUCKET}/${path}"
    URL="https://${ASUP_BUCKET}.s3.amazonaws.com/${path}"
    ###
    s3put "${fileToUpload}" "${resource}" 2>&1 | tee $LOGFILE
    rc=${PIPESTATUS[0]}
    if [ $rc -ne 0 ]; then
        echo
        syslog "ERROR($rc): Failed to upload $fileToUpload ($fullpath)"
    elif grep -q Error "${LOGFILE}"; then
        # rc can be 0 while the server sent us an error (file too big, no permissions, etc)
        echo
        syslog "ERROR: The server rejected $fileToUpload ($fullpath)"
        syslog "ERROR: $(cat $LOGFILE | tr -d '\n')"
        rc=1
    else
        syslog "Uploaded ${fullpath} to ${URL}"
    fi
    rm -f $LOGFILE $TMPFILE
    return $rc
}

ok () {
    local rc=$1
    shift
    if [ $rc -eq 0 ]; then
        echo "ok $testid - $*"
    else
        echo "not ok $testid - $*"
        failed=1
    fi
    testid=$((testid+1))
}

test_xcasup_upload () {
    local file="$1"
    local base=$(basename "$file" .tar.gz)

    ASUP_BUCKET_PREFIX="tests/"
    local dest="${ASUP_BUCKET_PREFIX}${DATEUNIX}/${HOSTNAME}/0/${base}.tar.gz"
    local dest_kms="${ASUP_BUCKET_PREFIX}${DATEUNIX}/${HOSTNAME}/0/${base}_kms.tar.gz"
    local s3dest="s3://${ASUP_BUCKET}/${dest}"
    local s3dest_kms="s3://${ASUP_BUCKET}/${dest_kms}"

    testid=1
    failed=0

    aws s3 rm --quiet "$s3dest"
    aws s3 rm --quiet "$s3dest_kms"

    echo "1..7"
    (set_keys && KMS=false xcasup_upload "$file" "$dest")
    ok $? "Uploaded unencrypted file"

    aws s3api head-object --bucket ${ASUP_BUCKET} --key ${dest} | grep -qv ServerSideEncryption
    ok $? "Check file is not encrypted"

    test $(curl -sSL -w '%{http_code}\n' -o /dev/null "$(aws s3 presign "$s3dest")") = 200
    ok $? "Able to fetch URL presigned via curl"

    (set_keys && xcasup_upload "$file" "$dest_kms")
    ok $? "Uploaded encrypted file"

    aws s3api head-object --bucket ${ASUP_BUCKET} --key ${dest_kms} | grep ServerSideEncryption | grep -q aws:kms
    ok $? "Check ServerSideEncryption KMS tag"

    res="$(curl -I -sSL -w '%{http_code}\n' -o /dev/null "$(aws s3 presign "$s3dest_kms")")"
    [[ $res =~ ^40 ]]
    ok $? "Unable to fetch encrypted presigned URL via curl (got $res)"

    return $failed
}

set_keys() {
    AWS_REGION=${AWS_REGION:-$ASUP_REGION}
    AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-$ASUP_BUCKET_KEY}
    AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-$ASUP_BUCKET_SECRET}"

    export AWS_REGION
    export AWS_ACCESS_KEY_ID
    export AWS_SECRET_ACCESS_KEY
}



if [ "$1" = -t ]; then
    shift
    DATEUNIX="0000000000"
    test $# -gt 0 || set -- "$XLRDIR/src/data/qa/caseStudy.tar.gz"
    test_xcasup_upload "$@"
else
    if test -z "$1"; then
        echo "Usage: $0 <filetoupload> ..." >&2
        exit 2
    fi

    if ! test -f "$1"; then
        echo "$0: Unable to open $1 for reading" >&2
        exit 2
    fi
    DATEUNIX=$(LC_ALL=C date +"%s")

    set_keys
    xcasup_upload "$1" "${ASUP_BUCKET_PREFIX}${DATEUNIX}/${HOSTNAME}/${2:-0}/$(basename $1)"
fi
