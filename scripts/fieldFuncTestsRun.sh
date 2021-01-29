#!/usr/bin/env xccli
# This is a set of functional tests which can be executed on customer prod
# installed systems. Here are the steps needed to perform this execution:
#
# MAKE SURE THE USER EXECUTING THESE STEPS HAS XCALAR ADMIN PRIVILEGE
#
# 0. Add /opt/xcalar/bin and /opt/xcalar/scripts to your path
#    e.g. "export PATH=/opt/xcalar/scripts:/opt/xcalar/bin:$PATH"
#
# 1. Run "fieldFuncTestsConfig.sh" on each node in the cluster in the
#    following manner:
#
#    First run this script on node 0, and after this script emits the line:
#        "Now please run fieldFuncTestsConfig.sh on other nodes"
#
#    only then, you should run this script on other nodes.
#    
#    On node 0, this script first stops Xcalar for all nodes, and then creates
#    the new config for functional tests, in the shared Xcalar root. It then
#    starts Xcalar (which waits for this script to be run on other nodes to
#    also start Xcalar and join the cluster).
#
#    On other nodes, this script will check that the new func test config is
#    available, and then starts Xcalar with the new config.
#
#    The Xcalar configuration (/etc/xcalar/default.cfg) is temporarily replaced
#    with a new configuration (in the Xcalar root directory), and included in it
#    are the functional test params (see /etc/xcalar/fieldFuncTests.cfg) needed
#    for the run.
#
#    This is a one-time operation (needed only once for every change to the
#    config).
#
#    Note that on node 0, this script will also check for any state left over
#    from a prior func test run (such as the xcalar rootdir used by the func
#    tests) - if such state is detected, it will abort and ask that
#    fieldFuncTestsClean.sh be run.
#
#    Note that since this script stops and starts Xcalar, it asks the user for
#    confirmation before proceeding, unless the "-y" parameter is specified: if
#    specified, it will skip interactive mode and proceed unconditionally
#    (useful if this script is to be executed in a batch mode).
#
# 2. Run "fieldFuncTestsRun.sh" on any node (the tests run on all
#    configured nodes). This will fail if step 1 wasn't executed, or Xcalar
#    isn't running for some reason.
#
# 3. The above step will output the results (Success or Failure) for all tests
#    to the console. For more details, grep for "Functional Test" in
#    /var/log/messages (but rate-limiting may cause some of these to be dropped)
#
# 4. Run "fieldFuncTestsClean.sh" ONLY on node 0 in the cluster to stop Xcalar
#    (which will stop Xcalar on all nodes), and clean out any state created by
#    step 2, and to revert the config back to the original state (i.e. reverse
#    the effects of step1 above). Only after this step is complete, can the
#    admin go back to step 1 and re-run a new batch of func tests. This script
#    since it stops Xcalar, proceeds only after getting OK from user running the
#    script, unless the "-y" parameter is specified: if specified, it will skip
#    interactive mode and proceed unconditionally.
#
functests run --allNodes --testCase libstat::statsStress
functests run --allNodes --testCase libbc::bcStress
functests run --allNodes --testCase libruntime::stress
functests run --allNodes --testCase libruntime::custom
functests run --allNodes --testCase libsession::sessionStress
functests run --allNodes --testCase liblog::logStress
functests run --allNodes --testCase liboperators::basicFunc
functests run --allNodes --testCase childfun::fun
functests run --allNodes --testCase libcallout::threads
functests run --allNodes --testCase libcallout::cancelStarvation
functests run --allNodes --testCase libdag::sanityTest
functests run --allNodes --testCase libdag::randomTest
functests run --allNodes --testCase libkvstore::kvStoreStress
functests run --allNodes --testCase libxdb::xdbStress
functests run --allNodes --testCase libmsg::msgStress
functests run --allNodes --testCase libqueryparser::queryParserStress
functests run --allNodes --testCase liboptimizer::optimizerStress
functests run --allNodes --testCase libapp::sanity
functests run --allNodes --testCase libapp::stress
functests run --allNodes --testCase libns::nsTest
functests run --allNodes --testCase libqm::qmStringQueryTest
functests run --allNodes --testCase liblocalmsg::sanity
functests run --allNodes --testCase liblocalmsg::stress
