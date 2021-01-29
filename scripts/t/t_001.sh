#!/bin/bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

#export UUID=${UUID:-$(uuidgen)}
export UUID=ca70253f-9002-4058-b4cf-4b6120ea6da3
export UUID_SHORT="${UUID%%-*}"
export WORKBUCKET=${WORKBUCKET:-abakshi-instamart-workbucket-1rei62296ezpe}
export EVENTPREFIX="${UUID}/events/"

W=s3://${WORKBUCKET}/${EVENTPREFIX%/}

xcalar_job() {
    python3 "${DIR}"/../xcalar_job.py "$@"
}

s3() {
    aws s3 "$@"
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

echo "TAP version 13"
echo "1..10"
xcalar_job -c -i "r5d.xlarge" -z 1 -j report01-cron -a MyReport -p "s3://${WORKBUCKET}/dataflows/MyReport.xlrapp.tar.gz" -m MyReport -s 'cron(0 8 * * ? *)' -r exportPath=josh-insta-workbucket-1c7judm3392qk/export/scheduled_updated_datamart.csv > report01-cron.json 2>err
tap $? MyReportCron

xcalar_job -c -i "r5d.xlarge" -z 1 -j report01-rate -a MyReport -p "s3://${WORKBUCKET}/dataflows/MyReport.xlrapp.tar.gz" -m MyReport -s 'rate(2 days)' -r exportPath=josh-insta-workbucket-1c7judm3392qk/export/scheduled_updated_datamart.csv > report01-rate.json 2>err
tap $? MyReportRate

[ $(xcalar_job -l | wc -l ) -eq 2 ]
tap $? JobCount1

! xcalar_job -c -i "r5d.xlarge" -z 1 -j -report01-badcron -a MyReport -p "s3://${WORKBUCKET}/dataflows/MyReport.xlrapp.tar.gz" -m MyReport -s 'rate(1 2 3)' -r exportPath=josh-insta-workbucket-1c7judm3392qk/export/scheduled_updated_datamart.csv > report01-badcron.json 2>err
tap $? MyReportBadCron

! xcalar_job -c -i "r5d.xlarge" -z 1 -j report01-badrate -a MyReport -p "s3://${WORKBUCKET}/dataflows/MyReport.xlrapp.tar.gz" -m MyReport -s 'cron(1 2 3)' -r exportPath=josh-insta-workbucket-1c7judm3392qk/export/scheduled_updated_datamart.csv > report01-badrate.json 2>err
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

