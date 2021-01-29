#!/bin/bash

pssh -i -h ~/edison.txt sudo chmod 0777 -R /opt/xcalar/bin
pssh -i -h ~/edison.txt 'pgrep gdbserver | xargs sudo kill -9'
pssh -i -h ~/edison.txt sudo service xcalar stop
pscp -h ~/edison.txt $XLRDIR/src/bin/usrnode/usrnode /opt/xcalar/bin/
pssh -i -h ~/edison.txt sudo service xcalar start
pssh -i -h ~/edison.txt 'pgrep usrnode | xargs sudo -E gdbserver localhost:12321 --attach'
