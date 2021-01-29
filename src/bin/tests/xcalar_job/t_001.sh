#!/bin/bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

#export UUID=${UUID:-$(uuidgen)}
export UUID=ca70253f-9002-4058-b4cf-4b6120ea6da3
export UUID_SHORT="${UUID%%-*}"
export WORKBUCKET=${WORKBUCKET:-abakshi-instamart-workbucket-1rei62296ezpe}
export EVENTPREFIX="${UUID}/events/"

W=s3://${WORKBUCKET}/${EVENTPREFIX%/}

xcalar_job() {
    python3 "${XLRDIR}"/scripts/xcalar_job.py "$@"
}

s3() {
    aws s3 "$@"
}

genjob() {
    local dataflow="$1"
    local dataflow_app="$2"
    local schedule="$3"
    if ! test -e "${dataflow}.xlrapp.tar.gz"; then
        s3 cp --quiet "$dataflow_app" "${dataflow}.xlrapp.tar.gz" >&2
    fi
    cat <<EOF
{
    "app" : {
        "path" : "${dataflow}.xlrapp.tar.gz",
        "dataflow" : "${dataflow}",
        "params" : [
            {"myfile" : "/xcfield/instantdatamart/mdmdemo/000fccb/c86cc8b/969cffb/fec195e/3c7bbdf/4d93b0a/a9f7e3e/24fb51.csv"},
            {"myfilter" : "90"},
            {"exppath" : "/${WORKBUCKET}/output/"}
        ]
    },
    "schedule" : "${schedule}"
}
EOF
}

test_idx=1
tap() {
    if [ $1 -eq 0 ]; then
        echo "ok $test_idx - $2"
    else
        echo "not ok $test_idx - $2"
    fi
    ((test_idx++))
}

set +e

s3 rm "$W"/ --recursive --quiet
echo "$W"/ | s3 cp --quiet - "$W"/self

genjob "MyReport" "s3://${WORKBUCKET}/dataflows/MyReport.xlrapp.tar.gz" 'cron(0 8 * * ? *)' > MyReportCron.json
genjob "MyReport" "s3://${WORKBUCKET}/dataflows/MyReport.xlrapp.tar.gz" 'rate(2 days)' > MyReportRate.json
genjob "MyReport" "s3://${WORKBUCKET}/dataflows/MyReport.xlrapp.tar.gz" 'rate(1 2 3)' > MyReportBadRate.json
genjob "MyReport" "s3://${WORKBUCKET}/dataflows/MyReport.xlrapp.tar.gz" 'cron(1 2 3)' > MyReportBadCron.json

echo "TAP version 13"
echo "1..10"
xcalar_job -c MyReportCron.json -j report01-cron -u > report01-cron.json 2>err
tap $? MyReportCron

xcalar_job -c MyReportRate.json -j report01-rate -u > report01-rate.json 2>err
tap $? MyReportRate

[ $(xcalar_job -l | wc -l ) -eq 2 ]
tap $? JobCount1

! xcalar_job -c MyReportBadCron.json -j report01-badcron -u > report01-badcron.json 2>err
tap $? MyReportBadCron

! xcalar_job -c MyReportBadRate.json -j report01-badrate -u > report01-badrate.json 2>err
tap $? MyReportBadRate

[ $(xcalar_job -l | wc -l ) -eq 2 ]
tap $? JobRecount1

xcalar_job -d report01-cron 2>err
tap $? DelCron

[ $(xcalar_job -l | wc -l ) -eq 1 ]
tap $? JobCountDown1

xcalar_job -d report01-rate 2>err
tap $? DelRate

[ $(xcalar_job -l | wc -l ) -eq 0 ]
tap $? JobCountDown0

