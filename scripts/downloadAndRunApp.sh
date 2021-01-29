#!/bin/bash
# downloadAndRunApp.sh - A script that downloads a packaged xlrapp.tar.gz file from S3 to your home directory and runs the application with parameters.
set -e
export XLRDIR=${XLRDIR:-/opt/xcalar}
export PATH=$XLRDIR/bin:$XLRDIR/scripts:$PATH
export HOME=${HOME:-$PWD}

if [ "$1" == -h ] || [ "$1" == --help ] || [ $# -ne 6 ]; then
  echo "Usage: '$0' [xcalar_username] [xcalar_password] [xcalar_app_name] [s3_app_location] [module_name] [params]"
  exit 0
else
  xc2 login --username "$1" --password "$2"
  xc2 workbook delete "$3" 
  /usr/bin/aws s3 cp "$4" $HOME/"$3".xlrapp.tar.gz
  xc2 workbook run --workbook-file $HOME/"$3".xlrapp.tar.gz --query-name "$3" --dataflow-name "$5" --params "$6"
fi