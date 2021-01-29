#!/bin/bash
#
# Installs the GCloud SDK
#
PROJECT="angular-expanse-99923"
REGION="us-central1"
ZONE="us-central1-f"
PROJECT_SVCACCOUNT_URL="https://console.cloud.google.com/permissions/serviceaccounts?project=${PROJECT}&authuser=1"
PROJECT_URL="https://console.coloud.google.com/home/dashboard?project=${PROJECT}"
GCLOUD_SDK_URL="https://sdk.cloud.google.com"
export PATH="$HOME/google-cloud-sdk/bin:$PATH"

echo >&2 "url           : $PROJECT_URL"
echo >&2 "project       : $PROJECT"
echo >&2 "compute/region: $REGION"
echo >&2 "compute/zone  : $ZONE"

if ! command -v gcloud >/dev/null; then
    export CLOUDSDK_CORE_DISABLE_PROMPTS=1
    set -o pipefail
    curl -sSL $GCLOUD_SDK_URL | bash -e
    if [ $? -ne 0 ]; then
        echo >&2 "Failed to install GCloud SDK from $GCLOUD_SDK_URL"
        exit 1
    fi
fi
test -r "$HOME/.bashrc" && . "$HOME/.bashrc"
echo >&2 "Please visit $PROJECT_URL to manually control GCE instances"
echo >&2 ""
echo >&2 "** You must run the following command to set up your GCloud SDK credentials **"
echo >&2 " \$ gcloud auth login"
echo >&2 ""
if [ "$ACTIVATE_SVCACCOUNT" = "1" ]; then
    echo >&2 "** If you want to activate a service account here, then please go here. If you don't know what that is then skip this step"
    echo >&2 "  $PROJECT_SVCACCOUNT_URL"
    echo >&2 "then run this:"
    echo >&2 " \$ gcloud auth activate-service-account $USER@${PROJECT}.iam.gserviceaccount.com --key-file myjsonfile.json"
fi
gcloud config set core/project $PROJECT
gcloud config set core/disable_usage_reporting true
gcloud config set compute/region $REGION
gcloud config set compute/zone $ZONE

