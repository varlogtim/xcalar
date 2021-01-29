#!/bin/bash

if [[ "$OSTYPE" =~ ^darwin ]]; then
    ifconfig | grep 'inet ' | grep broadcast | awk '{print $2}'
else
    hostname -i
fi
