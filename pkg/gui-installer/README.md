# GUI Installer

You can test all this in containers, make sure you've built trunk and build (or copy) an installer into $XLRDIR/build/

    cd $XLRDIR
    build clean
    build prod
    mkdir -p $XLRDIR/build
    INSTALLER=/netstore/builds/byJob/BuildTrunk/xcalar-latest-installer-prod
    INSTALLER_NAME=$(basename $(readlink -f $INSTALLER))
    TEST_NAME=<some-test-name>
    cp $INSTALLER  $XLRDIR/build/$INSTALLER_NAME
    cd $XLRDIR/pkg/gui-installer
    make
    ./test.sh

If you do not set the TEST_NAME above, then this will not be a test, and instead will be a way for field team to spin up docker containers to have xcalar running. In other words, it will keep this running. You will need to explicitly do a make clean in the docker directory to clean up after you are done.


This will build the xcalar-gui installer and install it into a 4 'node' ubuntu14 docker cluster

To change target OS, manually change pkg/gui-installer/tests/Makefile at the top OS ?= el7

