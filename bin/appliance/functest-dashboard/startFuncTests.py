#!/usr/bin/env python2.7
import os, sys
import time
import datetime
import ast
import socket
import os.path
import atexit
from threading import Thread
from argparse import ArgumentParser
import traceback
import copy
import syslog


class Xcalar:
    def __init__ (self, xccli="/opt/xcalar/bin/xccli"):
        self.xccli = xccli

    def is_up(self):
        res = os.popen(self.xccli + " -c 'version' --ip " + self.get_xcalar_ip()).read()[:-1]
        if "Connection refused" in res or "Backend Version" not in res:
            return False
        return True

    def get_build(self):
        try:
            res = os.popen("timeout 10s " + self.xccli + " -c 'version' --ip " + self.get_xcalar_ip()).read()[:-1]
            res = res.split('\n')[5].split('Backend Version: ')[1]
        except:
            res = ""
        return res

    def gdb_mode(self):
        return len(self.get_gdbserver()) != 0

    def get_gdbserver(self):
        res = os.popen("ps -ef | grep gdbserver | grep -v grep").read()[:-1]
        return res

    def get_xcalar_ip(self):
        res = os.popen("/sbin/ip route|awk '/default/ { print $3 }'").read()[:-1]
        return res



class FuncTest:
    def __init__ (self, testname=None, xccli="/opt/xcalar/bin/xccli"):
        self.testname = testname
        self.xccli = xccli
        self.xcalar = Xcalar(self.xccli)
        self.gdb_mode = False
        self.error = ""

    def run(self, functest_cmd):
        self.status = os.popen(functest_cmd).read()[:-1]

    def start(self, timeout=2*3600):
        functest_cmd = "%s -c 'functests run --allNodes --testCase %s' --ip %s"%(self.xccli, self.testname, self.xcalar.get_xcalar_ip())
        funcTestThread = Thread(target = self.run, args = (functest_cmd, ))
        funcTestThread.daemon = True
        funcTestThread.start()
        starttime = int(time.time())
        stop = False
        count = 1
        while(funcTestThread.isAlive()):
            # print "Test is still running"
            currenttime = int(time.time())
            if starttime + timeout < currenttime:
                syslog.syslog(syslog.LOG_WARNING, "Timeout, terminating the test")
                self.status = "Timedout"
                break
            elif self.xcalar.gdb_mode():
                syslog.syslog(syslog.LOG_WARNING, "gdbserver attached, exiting")
                self.status = "Error: Connection refused"
                self.gdb_mode = True
                self.error = self.xcalar.get_gdbserver()
                break
            else:
                count += 1
        return self.status

    def list(self):
        res = os.popen("%s -c 'functests list' --ip %s"%(self.xccli, self.xcalar.get_xcalar_ip())).read()[:-1]
        return res

    def get_params(self, cfgPath="/etc/xcalar/default.cfg"):
        res = os.popen("grep 'FuncTests' %s"%(cfgPath)).read()
        params = res.split('\n')
        active_params = []
        for param in params:
            if not param.startswith("#"):
                active_params.append(param)
        return active_params

global_all_status = None

def compare(test1, test2):
    global global_all_status
    if test1 in global_all_status and test2 not in global_all_status:
        return -1
    if test1 not in global_all_status and test2 in global_all_status:
        return 1
    if test1 not in global_all_status and test2 not in global_all_status:
        return 0
    time1 = datetime.datetime.strptime(global_all_status[test1]['start_time'], "%Y-%m-%d %H:%M:%S")
    time2 = datetime.datetime.strptime(global_all_status[test2]['start_time'], "%Y-%m-%d %H:%M:%S")
    if time1<time2:
        return 1
    if time1>time2:
        return -1
    return 0

def update_dashboard(allTests, all_status, html, host, prev_test_meta=None):
    global global_all_status
    global_all_status = all_status
    line = ""
    if prev_test_meta is not None:
        if prev_test_meta['status'] == 'Succeeded':
            color = "circle_green"
        elif prev_test_meta['status'] == 'Timedout':
            color = "circle_yellow"
        else:
            color = "circle_red"
        secline = ""
        secline = "<tr><td>%s</td><td class='%s'></td><td>%s</td><td>%s</td><td>%s</td></tr>\n"%(prev_test_meta['name'],
        color, "", prev_test_meta['start_time'], prev_test_meta['stop_time'])
    allTests = sorted(allTests, cmp=compare)

    i = 0
    for status in allTests:
        if status not in all_status:
            line += "<tr><td>%s</td><td class='circle_grey'></td><td>%s</td><td>%s</td><td>%s</td></tr>\n"%(status, "", "", "")
            continue

        duration_format = ""
        if all_status[status]['status'] != 'Started' and all_status[status]['status'] != 'Interrupted':
            # print all_status
            duration = all_status[status]['duration']
            duration_hour = duration / 3600
            duration_minute = (duration % 3600) / 60
            duration_second = (duration % 60)
            duration_format = str(duration_hour)+"h "+str(duration_minute)+"m "+str(duration_second)+"s"

        stop_time = ""
        if "stop_time" in all_status[status]:
            stop_time = all_status[status]['stop_time']

        color_class = ""

        if all_status[status]['status'] == 'Succeeded':
            color_class = "circle_green"
        elif all_status[status]['status'] == 'Timedout':
            color_class = "circle_yellow"
        elif all_status[status]['status'] == 'Started':
            color_class = "circle_blue"
        elif all_status[status]['status'] == 'Interrupted':
            color_class = "circle_purple"
        else:
            color_class = "circle_red"
        line += "<tr><td>%s</td><td class='%s'></td><td>%s</td><td>%s</td><td>%s</td></tr>\n"%(all_status[status]['name'], color_class,
            duration_format, all_status[status]['start_time'], stop_time)

        if i==0 and prev_test_meta is not None:
            line += secline
        i += 1

    indexhtml = open('%s/html/index_%s.html'%(target, host), 'w')
    indexhtml.write(html.replace('TOBE_REPLACED_FUNCTEST', line))
    indexhtml.close()

exit_execution = True
exit_allTests = None
exit_all_status = None
exit_test = None
exit_host = None
exit_html = None

@atexit.register
def goodbye():
    global exit_execution
    try:
        if exit_execution:
            exit_all_status[exit_test]['status'] = "Interrupted"
            all_status_write = open('%s/status/last_%s'%(target, exit_host), 'w')
            all_status_write.write(str(exit_all_status))
            all_status_write.close()
            update_dashboard(exit_allTests, exit_all_status, exit_html, exit_host)
            print "%s is interrupted on %s"%(exit_test, exit_host)
    except:
        pass

current_dir_path = os.path.dirname(os.path.realpath(__file__))
def init():
    if not os.path.exists(target+"/status"):
        os.makedirs(target+"/status")
    if not os.path.exists(target+"/stats"):
        os.makedirs(target+"/stats")
    if not os.path.exists(target+"/log"):
        os.makedirs(target+"/log")
    if not os.path.exists(target+"/html"):
        os.makedirs(target+"/html")

def main():
    global exit_execution
    global exit_allTests
    global exit_all_status
    global exit_test
    global exit_host
    global exit_html
    global target

    parser = ArgumentParser()
    parser.add_argument("-test", "--testCase", dest="testCase", type=str, action='append',
                        metavar="<testCase>", default=None,
                        help="FuncTest Name")
    parser.add_argument("-cliPath", "--cliPath", dest="cliPath", type=str,
                        metavar="<cliPath>", default="/opt/xcalar/bin/xccli",
                        help="CLI Path")
    parser.add_argument("-cfgPath", "--cfgPath", dest="cfgPath", type=str,
                        metavar="<cfgPath>", default="/etc/xcalar/default.cfg",
                        help="Config Path")
    parser.add_argument("-usrnode", "--usrnode", dest="usrnode", type=str,
                        metavar="<usrnode>", default="/opt/xcalar/bin/usrnode",
                        help="Usrnode location")
    parser.add_argument("-target", "--target", dest="target", type=str,
                        metavar="<target>", default="/netstore/users/xma/dashboard",
                        help="Location for all test data")

    args = parser.parse_args()
    tests = args.testCase
    xccli = args.cliPath
    cfg = args.cfgPath
    usrnode = args.usrnode
    target = args.target
    res = FuncTest(xccli=xccli).list()
    allTests = res.split('\n')[1:]
    if tests is None:
        tests = allTests

    init()
    TIMEOUT = 1209600

    host = socket.gethostname()
    customer = host

    print "%s: Test set started"%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    for test in tests:
        xcalar = Xcalar(xccli=xccli)
        if not xcalar.is_up():
            print "Xcalar is not started, exit now..."
            syslog.syslog(syslog.LOG_ERR, "Xcalar is not started, exit now...")
            sys.exit(1)
        
        # print allTests
        csv = "testId,clusterId,timestamp,testname,status\n"
        if os.path.isfile('%s/status/last_%s'%(target, host)):
            all_status = ast.literal_eval(open('%s/status/last_%s'%(target, host), 'r').read())
        else:
            all_status = {}
        i = 0

        line = ""
        params = FuncTest(xccli=xccli).get_params(cfgPath=cfg)[:-1]
        if len(params) == 0:
            line += "<li>No customized params</li>\n"
        else:
            for param in params:
                line += "<li>%s</li>\n"%(param)

        html = open('%s/html/index-customer-template.html'%(current_dir_path), 'r').read()

        if len(params) == 0:
            params = "No customized params"
        else:
            params_str = ""
            for param in params:
                params_str += param+"\n"
            params = params_str

        try:
            html = html.replace('TOBE_REPLACED_BUILD', xcalar.get_build())
        except:
            html = html.replace('TOBE_REPLACED_BUILD', "Filed to get version")

        html = html.replace('TOBE_REPLACED_CONFIG', line)
        html = html.replace('TOBE_REPLACED_CUSTOMER', customer)
        html = html.replace('TOBE_REPLACED_HOST', host)
        html = html.replace('TOBE_REPLACED_TIMEOUT', "%sh %sm %ss"%(str(TIMEOUT/3600), str((TIMEOUT%3600)/60), str(TIMEOUT%60)))

        syslog.syslog("Starting test: " + test)
        print "Starting test: " + test
        starttime = int(time.time())
        startdate = datetime.datetime.now()
        
        prev_status = "Not started"
        prev_test_meta = None
        if test in all_status:
            prev_status = all_status[test]['status']
            if "stop_time" in all_status[test]:
                prev_test_meta = copy.deepcopy(all_status[test])
        prev_githash = ""
        if test in all_status and "githash" in all_status[test]:
            prev_githash = all_status[test]['githash']

        all_status[test] = {}
        all_status[test]['githash'] = xcalar.get_build()
        all_status[test]['gdbserver'] = ''
        all_status[test]['name'] = test
        all_status[test]['start_time'] = str(startdate.strftime("%Y-%m-%d %H:%M:%S"))
        all_status[test]['status'] = "Started"
        update_dashboard(allTests, all_status, html, host, prev_test_meta)
        all_status_write = open('%s/status/last_%s'%(target, host), 'w')
        all_status_write.write(str(all_status))
        all_status_write.close()

        exit_execution = True
        exit_allTests = allTests
        exit_all_status = all_status
        exit_test = test
        exit_host = host
        exit_html = html
        functest = FuncTest(xccli=xccli, testname=test)
        res = functest.start(timeout=TIMEOUT)
        exit_execution = False

        if xcalar.gdb_mode():
            all_status[test]['gdbserver'] = functest.error

        url = ('http:/%s/html/index_%s.html'%(target, host)).replace('netstore', 'netstore.int.xcalar.com')

        endtime = int(time.time())

        if "Success" in res:
            res = "Succeeded"
        elif "Timedout" in res:
            res = "Timedout"
        elif "Error: Connection refused" in res:
            res = "Failed"
        else:
            res = "Failed"

        all_status[test]['status'] = res
        all_status[test]['duration'] = endtime-starttime
        all_status[test]['stop_time'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        all_status[test]['output'] = functest.status

        update_dashboard(allTests, all_status, html, host)

        all_status_write = open('%s/status/last_%s'%(target, host), 'w')
        all_status_write.write(str(all_status))
        all_status_write.close()

        time.sleep(1)

    syslog.syslog("%s: Test set finished"%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

if __name__ == "__main__":
    try:
        main()
    except:
        host = socket.gethostname()
        log = open('%s/log/log_%s'%(target, host), 'w')
        exc_type, exc_value, exc_tb = sys.exc_info()
        error =  str(traceback.format_exception(exc_type, exc_value, exc_tb))
        print error
        syslog.syslog(error)
        log.write(error)
        log.close()
        sys.exit(1)

