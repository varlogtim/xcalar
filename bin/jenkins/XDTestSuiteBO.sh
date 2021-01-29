#!/bin/bash
export XLRINFRADIR="${XLRINFRADIR:-$XLRDIR/xcalar-infra}"
export XLRGUIDIR="${XLRGUIDIR:-$XLRDIR/xcalar-gui}"
export XCE_LICENSEDIR=$XLRDIR/src/data
export XCE_LICENSEFILE=${XCE_LICENSEDIR}/XcalarLic.key
NUM_USERS=${NUM_USERS:-$(shuf -i 2-3 -n 1)}
TEST_DRIVER_PORT="5909"
NETSTORE=${NETSTORE:-"/netstore/qa/jenkins"}

genBuildArtifacts() {
    mkdir -p ${NETSTORE}/${TEST_TO_RUN}/${BUILD_ID}
    mkdir -p $XLRDIR/tmpdir

    # Find core files, dump backtrace & bzip core files into core.tar.bz2
    gdbcore.sh -c core.tar.bz2 $XLRDIR /var/log/xcalar /var/tmp/xcalar-root 2> /dev/null

    if [ -f core.tar.bz2 ]; then
        corefound=1
    else
        corefound=0
    fi

    find /tmp ! -path /tmp -newer /tmp/${TEST_TO_RUN}_${BUILD_ID}_START_TIME 2> /dev/null | xargs cp --parents -rt $XLRDIR/tmpdir/

    taropts="--warning=no-file-changed --warning=no-file-ignored --use-compress-prog=pbzip2"
    PIDS=()
    dirList=(tmpdir /var/log/xcalar /var/opt/xcalar/kvs)
    for dir in ${dirList[*]}; do
        if [ -d $dir ]; then
            case "$dir" in
                "/var/log/xcalar")
                    tar -cf var_log_xcalar.tar.bz2 $taropts $dir > /dev/null 2>&1 & ;;
                "/var/opt/xcalar/kvs")
                    tar -cf var_opt_xcalar_kvs.tar.bz2 $taropts $dir > /dev/null 2>&1 & ;;
                *)
                    tar -cf $dir.tar.bz2 $taropts $dir > /dev/null 2>&1 & ;;
            esac
            PIDS+=($!)
        fi
    done

    wait "${PIDS[@]}"
    local ret=$?
    if [ $ret -ne 0 ]; then
        echo "tar returned non-zero value"
    fi

    for dir in core ${dirList[*]}; do
        case "$dir" in
            "/var/log/xcalar")
                cp var_log_xcalar.tar.bz2 ${NETSTORE}/${TEST_TO_RUN}/${BUILD_ID}
                rm var_log_xcalar.tar.bz2
                rm $dir/* 2> /dev/null
                ;;
            "/var/opt/xcalar/kvs")
                cp var_opt_xcalar_kvs.tar.bz2 ${NETSTORE}/${TEST_TO_RUN}/${BUILD_ID}
                rm var_opt_xcalar_kvs.tar.bz2
                rm $dir/* 2> /dev/null
                ;;
            *)
                if [ -f $dir.tar.bz2 ]; then
                    cp $dir.tar.bz2 ${NETSTORE}/${TEST_TO_RUN}/${BUILD_ID}
                    rm $dir.tar.bz2
                    if [ -d $dir ]; then
                        rm -r $dir/* 2> /dev/null
                    fi
                fi
                ;;
        esac
    done

    return $corefound
}

collectFaildLogs() {
    mkdir -p /var/log/xcalar/failedLogs || true
    cp -r "/tmp/xce-`id -u`"/* /var/log/xcalar/failedLogs/
    cp $TmpCaddyLogs /var/log/xcalar/failedLogs/

    CADDYHOME="$TmpCaddyDir" "$XLRDIR"/bin/caddy.sh stop
}

onExit() {

    local retval=$?
    set +e

    if [[ $retval != 0 ]]
    then
        collectFaildLogs
        genBuildArtifacts
        echo "Build artifacts copied to ${NETSTORE}/${TEST_TO_RUN}/${BUILD_ID}"
    fi

    exit $retval
}

trap onExit SIGINT SIGTERM EXIT

storeExpServerCodeCoverage() {
    outputDir=/netstore/qa/coverage/${TEST_TO_RUN}/${BUILD_ID}
    mkdir -p "$outputDir"
    covReportDir=$XLRGUIDIR/xcalar-gui/services/expServer/test/report

    if [ -d "$covReportDir" ]; then
        echo "expServer code coverage report copied to ${outputDir}"
        cp -r "$covReportDir"/* "${outputDir}"
    else
        echo "code coverage report folder doesn't exist on ${covReportDir}"
    fi
}

storeXDUnitTestCodeCoverage() {
    covReport=$XLRGUIDIR/assets/dev/unitTest/coverage/coverage.json
    if [ -f "$covReport" ]; then
        outputDir=/netstore/qa/coverage/${TEST_TO_RUN}/${BUILD_ID}
        mkdir -p "$outputDir"
        echo "XDUnitTest coverage report copied to ${outputDir}"
        cp -r "${covReport}" "${outputDir}"
        gzip "$outputDir/coverage.json"
        # Record the branch configurations
        echo "XCE_GIT_BRANCH: $XCE_GIT_BRANCH" > "$outputDir/git_branches.txt"
        echo "XD_GIT_BRANCH: $XD_GIT_BRANCH" >> "$outputDir/git_branches.txt"
        echo "INFRA_GIT_BRANCH: $INFRA_GIT_BRANCH" >> "$outputDir/git_branches.txt"
    else
        echo "code coverage report doesn't exist at ${covReport}"
    fi
}

runExpServerIntegrationTest() {
    set +e
    xc2 cluster stop # Stop the one node cluster that was started for GerritExpServerTest, to let pytest start a cluster on its own.
    currentDir=$PWD
    local retval=0

    cd $XLRDIR/src/bin/tests/pyTestNew
    testCases=("test_dataflow_service.py" "test_workbooks_new" "test_dfworkbooks_execute.py" "test_dataflows_execute.py")

    echo "running integration test for expServer"
    for testCase in "${testCases[@]}"; do
        echo "running test $testCase"
        ./PyTest.sh -k "$testCase"
        local ret=$?
        if [ $ret -ne "0" ]; then
            retval=1
            break
        fi
    done

    cd $currentDir
    set -e
    return $retval
}

if [ $TEST_TO_RUN = "GerritSQLCompilerTest" ]; then
    cd $XLRGUIDIR
    git diff --name-only HEAD^1 > out
    echo `cat out`
    diffTargetFile=`cat out | grep -E "(assets\/test\/json\/SQLTest-a.json|assets\/extensions\/ext-available\/sql.ext|ts\/components\/sql\/|ts\/thrift\/XcalarApi.js|ts\/shared\/api\/xiApi.ts|ts\/components\/worksheet\/oppanel\/SQL.*|ts\/XcalarThrift.js|ts\/components\/dag\/node\/.*SQL.*|ts\/components\/dag\/(DagView.ts|DagGraph.ts|DagSubGraph.ts|DagTab.ts|DagTabManager.ts|DagTabSQL.ts))" | grep -v "ts\/components\/sql\/sqlQueryHistoryPanel.ts"`

    rm -rf out

    if [ -n "$diffTargetFile" ] || [ "$RUN_ANYWAY" = "true" ]
    then
        echo "Change detected"
    else
        echo "No target file changed"
        exit 0
    fi
fi

install_reqs() {
    echo "Installing required packages"
    if grep -q Ubuntu /etc/os-release; then
        sudo apt-get install -y libnss3-dev chromium-browser
        sudo apt-get install -y libxss1 libappindicator1 libindicator7 libgconf-2-4
        sudo apt-get install -y Xvfb
        if [ ! -f /usr/bin/chromedriver ]; then
            echo "Wget chrome driver"
            wget http://chromedriver.storage.googleapis.com/2.24/chromedriver_linux64.zip
            unzip chromedriver_linux64.zip
            chmod +x chromedriver
            sudo mv chromedriver /usr/bin/
        else
            pwd
            echo "Chrome driver already installed"
        fi
    else
        sudo curl -ssL http://repo.xcalar.net/rpm-deps/google-chrome.repo | sudo tee /etc/yum.repos.d/google-chrome.repo
        curl -sSO https://dl.google.com/linux/linux_signing_key.pub
        sudo rpm --import linux_signing_key.pub
        rm linux_signing_key.pub
        sudo yum install -y google-chrome-stable
        sudo yum localinstall -y /netstore/infra/packages/chromedriver-2.34-2.el7.x86_64.rpm
        sudo yum install -y Xvfb
    fi
}

pip install pyvirtualdisplay selenium

echo "Starting Caddy"
pkill caddy || true
TmpCaddyDir=`mktemp -d -t Caddy.XXXXXX`
TmpCaddy=$TmpCaddyDir/Caddyfile
TmpCaddyLogs="$TmpCaddyDir/caddy*.log"
cp $XLRDIR/conf/Caddyfile-dev "$TmpCaddy"
# strip out the jwt settings for testing (for now) to allow unauthenticated access
sed -i -e '/jwt {/,/}/d' "$TmpCaddy"

CADDYHOME="$TmpCaddyDir" "$XLRDIR"/bin/caddy.sh start -c "$TmpCaddy" -p 8443

export NODE_ENV=dev
export XCE_CONFIG="${XCE_CONFIG:-$XLRDIR/src/data/test.cfg}"
xc2 cluster start --num-nodes 1

if [ $TEST_TO_RUN = "XDEndToEndTest" ]; then
    cd $XLRGUIDIR/assets/dev/e2eTest
    npm install
else
    cd $XLRGUIDIR/assets/dev/unitTest
    # Please don't ask me why I have to independently install this package.
    # This is the only way I've found to make it work.
    npm install node-bin-setup
    npm install
fi

curl -s http://localhost:27000/xcesql/info |jq '.'
exitCode=1
echo "Starting test driver"
if  [ $TEST_TO_RUN = "GerritSQLCompilerTest" ]; then
    npm test -- sqlTest https://localhost:8443
    exitCode=$?
elif [ $TEST_TO_RUN = "XDUnitTest" ]; then
    npm test -- unitTest https://localhost:8443
    exitCode=$?
    if [ "$STORE_COVERAGE" = "true" ]; then
        storeXDUnitTestCodeCoverage
    fi
elif [ $TEST_TO_RUN = "GerritExpServerTest" ]; then
    npm test -- expServer https://localhost:8443
    exitCode=$?
    if [ "$STORE_COVERAGE" = "true" ]; then
        storeExpServerCodeCoverage
    fi
    if [ $exitCode = "0" ]; then
        runExpServerIntegrationTest
        exitCode=$?
    fi
elif [ $TEST_TO_RUN = "GerritXcrpcIntegrationTest" ]; then
    npm test -- xcrpcTest https://localhost:8443
    exitCode=$?
elif [ $TEST_TO_RUN = "XDTestSuite" ]; then
    npm test -- testSuite https://localhost:8443
    exitCode=$?
elif [ $TEST_TO_RUN = "XDEndToEndTest" ]; then
    npm test -- --tag "allTests" --env jenkins
    exitCode=$?
elif [ $TEST_TO_RUN = "XDFuncTest" ]; then
    npm test -- XDFuncTest https://localhost:8443 $NUM_USERS $ITERATIONS
    exitCode=$?
fi

xc2 cluster stop

exit $exitCode
