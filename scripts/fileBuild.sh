#!/bin/bash

sed "/_XCE_CONF_FILE_TOKEN_/ {
	r $1
	d
}" $2
