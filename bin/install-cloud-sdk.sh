#!/bin/bash

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
export PATH="$DIR:$PATH"
. osid >/dev/null
VERSTRING="$(_osid --full)"

set -e

case "$VERSTRING" in
	ub14)
		# Create an environment variable for the correct distribution
		export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)"
		# Add the Cloud SDK distribution URI as a package source
		echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list
		# Import the Google Cloud public key
		curl -sL https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
		# Update and install the Cloud SDK
		sudo apt-get update -q && sudo DEBIAN_FRONTEND=noninteractive apt-get install -yqq google-cloud-sdk
		# Run gcloud init to get started
		;;
	*)
		curl https://sdk.cloud.google.com | bash
		;;
esac

rc=$?
if [ $rc -eq 0 ]; then
    echo >&2 "Run gcloud init to get started"
fi
exit $rc

