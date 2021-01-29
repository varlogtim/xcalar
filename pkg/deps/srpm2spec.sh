#!/bin/bash
if [ -n "$1" ] && [ -e "$1" ]; then
	rpm2cpio "$1" | cpio -civ '*.spec'
else
	echo >&2 "usage: $0 foo.src.rpm"
	exit 1
fi
