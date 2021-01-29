import re
import sys
import time
import shlex
import signal
import datetime
import subprocess


# XXX version < 10.X ... -G after 11.X
pidstat_cmd = 'pidstat -rh -C {cmd_str}'
time_format = '%Y-%m-%d %H:%M:%S'
fts = datetime.datetime.fromtimestamp

# Goals:
# - Track childnode pids memory usage
# - Show mem gain per sample
# - Show running average mem gain
# - Show report at end (ctrl-c)


USAGE = """
Purpose:
 - Runs a loop. Stop with SIGINT (ctrl+c)
 - Gather memory usage for a set of process matching <cmd_str_part>
 - Report changes in memory between iterations.
 - Report final statistics over life of execution.
Usage:
 - {script} <cmd_str_part>

Notes:
 - Intented for pidstat 10.1 (write version check)
 - I.e., works on RHEL 7, not on Ubuntu
 - If this command is piped you need to trap the SIGINT. Example:

 python {script} childnode | (trap '' INT; while read line; do echo "$line" | tee -a /var/log/xcalar/childnode.memory.use.log; done)

""".format(script=sys.argv[0])

data = {}
# {
#   <pid>: [
#       {
#           <time>: (int) <epoch>,
#           <RSS>: (int) <RES MEM INSTANT>,
#           <VSZ>: (int) <VIRT MEM INSTANT>,
#           <RSS_diff>: (int) <RES MEM diff last iter>,
#           <VSZ_diff>: (int) <VIRT MEM diff last iter>,
#       }, ... append as time passes ...],
# }


def err(msg):
    sys.stderr.write(f"{msg}")
    sys.stderr.flush()


header_report = {
    "max_rss_time": "Max RSS Time",
    "max_rss": "Max RSS",
    "max_rss_diff_time": "Max RSS dT",
    "max_rss_diff": "Max RSS d",
    "num_entries": "Num Entries"
}


def print_report_entry(report):
    print(
        f"{report['max_rss_time']: <22}"
        f"{report['max_rss']: <12}"
        f"{report['max_rss_diff_time']: <22}"
        f"{report['max_rss_diff']: >10}"
        f"{report['num_entries']: >12}"
    )


def do_report():
    print(f"{' report':->40}{' ':-<40}")
    for pid, entries in data.items():
        print(f"{' pid: ' + pid:->40}{' ':-<40}")
        num_enties = len(entries)
        max_rss = 0
        max_rss_time = 0
        max_rss_diff = 0
        max_rss_diff_time = 0
        max_rss_diff_time = 0
        for entry in entries:
            if entry["RSS"] >= max_rss:
                max_rss = entry["RSS"]
                max_rss_time = entry["time"]
            if entry["RSS_diff"] >= max_rss_diff:
                max_rss_diff = entry["RSS_diff"]
                max_rss_diff_time = entry["time"]

        report = {
            "max_rss_time": max_rss_time,
            "max_rss": max_rss,
            "max_rss_diff_time": max_rss_diff_time,
            "max_rss_diff": max_rss_diff,
            "num_entries": num_enties
        }
        print_report_entry(header_report)
        print_report_entry(report)


# #      Time   UID       PID  minflt/s  majflt/s     VSZ    RSS   %MEM  Command
#  1595972615  1000        83      0.02      0.00   16300   3608   0.01  bash
pidstat_line_regex = re.compile(
    r'\s*(?P<time>[0-9]+)\s+(?P<UID>\d+)\s+'
    r'(?P<PID>\d+)\s+(?P<minflt>[0-9\.]+)\s+(?P<majflt>[0-9\.]+)\s+'
    r'(?P<VSZ>\d+)\s+(?P<RSS>\d+)\s+(?P<MEM>[0-9\.]+)\s+(?P<cmd>.+)\s*'
)
header_update = {
    "time": "time",
    "VSZ": "VSZ",
    "RSS": "RSS",
    "VSZ_diff": "VSZ_diff",
    "RSS_diff": "RSS_diff",
    "pid": "pid"
}


def print_pid_update(update):
    print(
        f'{update["time"]: <20} {update["pid"]: <6}   '
        f'{update["RSS"]: <13} {update["VSZ"]: <13} '
        f'{update["RSS_diff"]: <12} {update["VSZ_diff"]: <12}'
    )


def update_data(pid_dict):
    global data
    pid = pid_dict["PID"]
    vsz_diff = 0
    rss_diff = 0

    if not data.get(pid):
        data[pid] = []
    if len(data[pid]) >= 1:
        prev = data[pid][-1]
        vsz_diff = int(pid_dict["VSZ"]) - int(prev["VSZ"])
        rss_diff = int(pid_dict["RSS"]) - int(prev["RSS"])
    ds = fts(int(pid_dict["time"])).strftime(time_format)
    update = {
        "time": ds,
        "VSZ": int(pid_dict["VSZ"]),
        "RSS": int(pid_dict["RSS"]),
        "VSZ_diff": int(vsz_diff),
        "RSS_diff": int(rss_diff),
        "pid": int(pid)}
    print_pid_update(update)
    data[pid].append(update)


def pid_gen(pidstat_out):
    for line in pidstat_out.split('\n'):
        m = re.search(pidstat_line_regex, line)
        if m is None:
            continue
        yield m.groupdict()


def pidstat():
    args = shlex.split(pidstat_cmd)
    while 1:
        p = subprocess.Popen(args, stdout=subprocess.PIPE)
        pout = p.stdout.read().decode('utf-8')
        print('-' * 80)
        print_pid_update(header_update)
        for pid_data in pid_gen(pout):
            update_data(pid_data)
        time.sleep(1)


def sig_hand(sig, frame):
    print()
    do_report()
    sys.stdout.buffer.write("\x04")
    sys.exit(1)


def arg_check():
    global pidstat_cmd
    if len(sys.argv) != 2:
        err(USAGE)
        sys.exit(1)
    pidstat_cmd = pidstat_cmd.format(cmd_str=sys.argv[1])


signal.signal(signal.SIGINT, sig_hand)
arg_check()
pidstat()
