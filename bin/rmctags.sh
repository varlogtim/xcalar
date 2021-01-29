#!/bin/bash

find src -iname tags -exec rm -f {} \;
rm -f cscope.in.out cscope.out cscope.po.out
