addtaptest ./mgmtdtest.sh
addtaptest ./monitorTest.sh
addtaptest ./cliTest.sh
# Disabled via Xc-12698
# addTest ./monitorBigTest.sh
addtaptest ./licCheckTest.sh
addtaptest ./sessionReplayTest.sh
addtaptest ./portScanTest.sh
addtaptest ./localSystemTest.sh
addtaptest ./runXceTests.sh
subdir pyTest
subdir pyTestNew
