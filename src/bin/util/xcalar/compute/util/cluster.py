"""Manage a Xcalar cluster

Provides for launching, querying, stopping a Xcalar cluster. This is tightly
tied to the configuration for that cluster. The module for interacting with
the configuration is located in .config, a sister module to this one.
"""
import os
import fnmatch
import sys
import stat
import subprocess
import time
import shutil
import logging
import tempfile
import datetime
import signal
import enum
import re

import requests
import psutil

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.client import Client

import xcalar.compute.util.config

DEF_CLUSTER_START_TIMEOUT = 300
DEF_CLUSTER_TERM_TIMEOUT = 120
DEF_CLUSTER_KILL_TIMEOUT = 60
DEF_NUM_NODES = 3

EXPSERVER_PATH = "xcalar-gui/services/expServer"
SQLDF_DEV_PATH = "src/sqldf/sbt/target/xcalar-sqldf.jar"
SQLDF_PROD_PATH = "lib/xcalar-sqldf.jar"

XLR_PREFIX = "/opt/xcalar"

xc2debug = os.environ.get('XC2DEBUG')

# Set up logging
logger = logging.getLogger("Cluster")
logger.setLevel(logging.INFO)
if not logger.handlers:
    log_handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(
        '%(asctime)s - Cluster - %(levelname)s - %(message)s')
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)


# Process states
class ProcessState(enum.Enum):
    DOWN = "down"
    STARTED = "started"
    UP = "up"


def _getPids(procNames):
    for p in psutil.process_iter():
        try:
            name = p.name()
        except psutil.NoSuchProcess:
            continue
        if name in procNames:
            yield p.pid


def _parse_arg(cmdline, short_arg=None, long_arg=None):
    """Parse a cmdline argument value

    cmdline is an array of the command line ['usrnode', '-i', '0']
    short_arg is like '-i'
    long_arg is like '--nodeId'
    Returns None or the first value it finds for either the short or long arg
    """
    assert short_arg or long_arg

    if short_arg and short_arg in cmdline:
        arg_index = cmdline.index(short_arg)
    elif long_arg and long_arg in cmdline:
        arg_index = cmdline.index(long_arg)
    else:
        return None

    if arg_index + 1 >= len(cmdline):
        return None

    arg_val = cmdline[arg_index + 1]
    return arg_val


def _parse_int(s):
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def _parse_usrnode_cmdline(cmdline):
    """Parse out (node_id, num_nodes, config_file) or None from cmdline

    Returns None if it is not a valid usrnode cmdline.
    Otherwise returns a tuple of the above form. All elements can be None
    """
    if not cmdline or cmdline[0] != "usrnode":
        return None

    node_id = _parse_int(_parse_arg(cmdline, "-i", "--nodeId"))
    num_nodes = _parse_int(_parse_arg(cmdline, "-n", "--numNodes"))
    config_file = _parse_arg(cmdline, "-f", "--configFile")

    return (node_id, num_nodes, config_file)


def _parse_xcmonitor_cmdline(cmdline):
    """Parse out (node_id, num_nodes, config_file) or None from cmdline

    Returns None if it is not a valid xcmonitor cmdline.
    Otherwise returns a tuple of the above form. All elements can be None
    """
    if not cmdline or cmdline[0] != "xcmonitor":
        return None

    node_id = _parse_int(_parse_arg(cmdline, "-n"))
    num_nodes = _parse_int(_parse_arg(cmdline, "-m"))
    config_file = _parse_arg(cmdline, "-c")

    return (node_id, num_nodes, config_file)


def _parse_xcmgmtd_cmdline(cmdline):
    """Parse out (config_file) or None from cmdline

    Returns None if it is not a valid xcmgmtd cmdline.
    Otherwise returns a tuple of the above form.
    """
    if len(cmdline) < 2 or cmdline[0] != "xcmgmtd":
        return None

    return (cmdline[1])


def _parse_exp_server_cmdline(cmdline):
    """Parse out (exp_server_path) or None from cmdline

    Returns None if it is not a valid expServer cmdline.
    Otherwise returns a tuple of the above form.
    """
    if len(cmdline) < 2 or cmdline[0] != "node":
        return None

    node_program = cmdline[-1]
    if "expServer.js" not in node_program:
        return None

    return (node_program)


def _parse_jupyter_notebook_cmdline(cmdline):
    """Parse out jupyter-notebook or None from cmdline

    Returns None if it is not a valid jupyter-notebook cmdline.
    Otherwise returns True
    """
    if len(cmdline) < 2 or "python" not in cmdline[0]:
        return None

    py_program = cmdline[1]
    if "jupyter-notebook" not in py_program:
        return None

    return True


def _parse_sqldf_cmdline(cmdline):
    """Parse out (sqldf_jar_path) or None from cmdline

    Returns None if it is not a valid sqldf cmdline.
    Otherwise returns a tuple of the above form.
    """
    if len(cmdline) < 3 or cmdline[0] != "java":
        return None

    jar_cmd = cmdline[1]
    jar_file = cmdline[2]
    if jar_cmd != "-jar" or "xcalar-sqldf.jar" not in jar_file:
        return None

    return (jar_file)


def _check_for_cores():
    xce_binaries = ["xcmonitor", "usrnode", "xcmgmtd", "childnode"]
    for xce_binary in xce_binaries:
        lines = subprocess.check_output(
            "find -name 'core.{}.*'".format(xce_binary),
            shell=True).splitlines()
        if len(lines) > 0:
            raise RuntimeError("Coredumps found at {}".format(", ".join(
                map(str, lines))))


def _is_process_core_dumping(pid):
    try:
        lines = subprocess.check_output(
            "cat /proc/{}/stat | cut -d\\  -f3".format(pid),
            shell=True).splitlines()
    except subprocess.CalledProcessError as e:
        return False
    for line in lines:
        return line == b"D"
    return False


def _xce_processes():
    """Produce all running xce processes"""
    now = time.time()
    for proc in psutil.process_iter():
        proc_info = {}
        try:
            cmdline = proc.cmdline()
            name = proc.name()
        except psutil.NoSuchProcess:
            # anything with subprocesses is inherently racy; just ignore
            # anything that disappears
            continue

        if name == "xcmonitor":
            if cmdline:
                tup = _parse_xcmonitor_cmdline(cmdline)
                if tup:
                    node_id, num_nodes, config_file = tup
                    assert (node_id is not None)
                    proc_info["name"] = "xcmonitor {}".format(node_id)
                else:
                    continue
            else:
                # Zombie has no command line but need to still track for status
                proc_info["name"] = "xcmonitor"    # No node_id
        elif _parse_usrnode_cmdline(cmdline):
            node_id, num_nodes, config_file = _parse_usrnode_cmdline(cmdline)
            proc_info["name"] = "usrnode {}".format(node_id)

        elif _parse_xcmgmtd_cmdline(cmdline):
            proc_info["name"] = "xcmgmtd"

        elif _parse_exp_server_cmdline(cmdline):
            proc_info["name"] = "expServer"

        elif _parse_sqldf_cmdline(cmdline):
            proc_info["name"] = "sqldf"

        else:
            continue
        proc_info["cmdline"] = cmdline
        proc_info["pid"] = proc.pid
        create_time = proc.create_time()
        uptime = datetime.timedelta(seconds=now - create_time)
        proc_info["uptime"] = str(uptime)
        proc_info["status"] = ProcessState.STARTED
        yield proc_info


# Find expServer
def _potential_exp_server_paths():
    """Produce all potential expServer paths in order of priority"""
    if "XLRGUIDIR" in os.environ:
        xlr_gui = os.environ["XLRGUIDIR"]
        yield os.path.join(xlr_gui, EXPSERVER_PATH)


def _find_exp_server_path():
    """Produce the path to expServer.js or throw a RuntimeError"""
    for path in _potential_exp_server_paths():
        if path and os.path.isdir(path):
            return path
    raise RuntimeError("ExpServer not found")


# Find sqldf
def _potential_sqldf_paths():
    """Produce all potential sqldf paths in order of priority"""
    xlr_dir = os.environ["XLRDIR"]
    yield os.path.join(xlr_dir, SQLDF_DEV_PATH)
    yield os.path.join(xlr_dir, SQLDF_PROD_PATH)


def _find_sqldf_path():
    """Produce the path to xcalar-sqldf.jar or throw a RuntimeError"""
    for path in _potential_sqldf_paths():
        if path and os.path.isfile(path):
            return path
        else:
            logger.debug("sqldf not found at {}".format(path))
    raise RuntimeError("sqldf not found")


def _set_jupyter_config_path():
    """setup jupyter path to link to configuration from xcalar gui folder"""
    env = os.environ
    if "XLRGUIDIR" not in env:
        logger.error("Failed to setup jupyter path")
        raise RuntimeError("XLRGUIDIR env variable not set")
    # Using the environment variable overrides any existing ~/.jupyter or
    # ~/.ipython
    xd_jupyter_dir = os.path.join(env["XLRGUIDIR"],
                                  "xcalar-gui/assets/jupyter/jupyter")
    os.environ['JUPYTER_CONFIG_DIR'] = xd_jupyter_dir

    xd_ipython_dir = os.path.join(env["XLRGUIDIR"],
                                  "xcalar-gui/assets/jupyter/ipython")
    os.environ['IPYTHONDIR'] = xd_ipython_dir


# Find running cluster
def detect_cluster():
    procs = list(_xce_processes())
    potential_configs = set()
    potential_num_nodes = set()
    for proc in procs:
        if _parse_xcmonitor_cmdline(proc["cmdline"]):
            node_id, num_nodes, config_file = _parse_xcmonitor_cmdline(
                proc["cmdline"])
            logger.debug(
                "detected xcmonitor {}(pid {}) with num_nodes {}; config_file {}"
                .format(node_id, proc["pid"], num_nodes, config_file))

            potential_configs.add(os.path.realpath(config_file))
            if num_nodes is not None:
                potential_num_nodes.add(num_nodes)

        elif _parse_usrnode_cmdline(proc["cmdline"]):
            node_id, num_nodes, config_file = _parse_usrnode_cmdline(
                proc["cmdline"])
            logger.debug(
                "detected usrnode {}(pid {}) with num_nodes {}; config_file {}"
                .format(node_id, proc["pid"], num_nodes, config_file))

            potential_configs.add(os.path.realpath(config_file))
            if num_nodes is not None:
                potential_num_nodes.add(num_nodes)

        elif _parse_xcmgmtd_cmdline(proc["cmdline"]):
            config_file = _parse_xcmgmtd_cmdline(proc["cmdline"])
            logger.debug("detected xcmgmtd(pid {}) with config_file {}".format(
                proc["pid"], config_file))
            potential_configs.add(os.path.realpath(config_file))

    if len(potential_configs) > 1:
        logger.warning("{} potential config files found: {}".format(
            len(potential_configs), list(potential_configs)))

    if len(potential_num_nodes) > 1:
        logger.warning("{} potential cluster sizes found: {}".format(
            len(potential_num_nodes), list(potential_num_nodes)))

    cluster_args = {}

    # Use the config file we found if available, fall back to 'detect_config'
    if potential_configs:
        logger.debug("config files of running processes: {}".format(
            list(potential_configs)))
        cluster_args["config_path"] = next(iter(potential_configs))

    # Use the cluster size we found if available, fall back to default
    if potential_num_nodes:
        logger.debug("cluster sizes of running processes: {}".format(
            list(potential_num_nodes)))
        cluster_args["num_nodes"] = next(iter(potential_num_nodes))

    return DevCluster(**cluster_args)


# An instance of this class embodies a cluster which is configured but may or
# may not be currently running
class DevCluster:
    def __init__(self,
                 num_nodes=DEF_NUM_NODES,
                 config_path=None,
                 guardrailsArgs=None):
        if config_path:
            self._config = xcalar.compute.util.config.build_config(config_path)
        else:
            self._config = xcalar.compute.util.config.detect_config()
        logger.debug("using config file '{}'".format(
            self._config.config_file_path))

        if guardrailsArgs is None:
            guardrailsArgs = os.environ.get("GuardRailsArgs")

        if num_nodes > self._config.num_nodes:
            raise ValueError(
                "{} nodes requested but only {} configured in {}".format(
                    num_nodes, self._config.num_nodes,
                    self._config.config_file_path))
        self._num_nodes = num_nodes
        if "container" in os.environ:
            self._use_cgroups = False
        else:
            self._use_cgroups = self._config.all_options.get(
                "Constants.Cgroups", "true").lower() == "true"

        self._guardrailsArgs = guardrailsArgs

        self.usrnode_unit_path = "xcalar.slice/xcalar-usrnode.service"
        if os.path.isdir("/cgroup/cpu"):
            self.cgroup_path_dict = {
                "cpu": "/cgroup/cpu",
                "cpuacct": "/cgroup/cpuacct",
                "memory": "/cgroup/memory",
                "cpuset": "/cgroup/cpuset"
            }
        elif os.path.isdir("/sys/fs/cgroup/cpu,cpuacct"):
            self.cgroup_path_dict = {
                "cpu": "/sys/fs/cgroup/cpu,cpuacct",
                "cpuacct": "/sys/fs/cgroup/cpu,cpuacct",
                "memory": "/sys/fs/cgroup/memory",
                "cpuset": "/sys/fs/cgroup/cpuset"
            }
        else:
            self.cgroup_path_dict = {
                "cpu": "/sys/fs/cgroup/cpu",
                "cpuacct": "/sys/fs/cgroup/cpuacct",
                "memory": "/sys/fs/cgroup/memory",
                "cpuset": "/sys/fs/cgroup/cpuset"
            }

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        self.stop()

    # Let's give this 2 minutes to boot and report failure if there is timeout.
    # Most of the jenkins slaves are poorly resourced and our pre-checkin tests
    # tend to start swapping the jenkins slave anyway!
    def start(self, clean_root=False, timeout=DEF_CLUSTER_START_TIMEOUT):
        timeout = int(
            os.environ.get("TIME_TO_WAIT_FOR_CLUSTER_START", timeout))
        """Start a Xcalar cluster or throw a RuntimeError"""
        if self.is_up():
            logger.warning("XCE already up; doing nothing")
            return

        if clean_root:
            root_path = self._config.xcalar_root_path
            for fn in os.listdir(root_path):
                path = os.path.join(root_path, fn)
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)

        # Set up the default admin file for development environments
        admin_json_path = os.path.join(self._config.xcalar_root_path,
                                       "config/defaultAdmin.json")
        if not os.path.exists(admin_json_path):
            logger.warning("No admin config found")
            default_admin_script_path = os.path.join(
                os.environ["XLRDIR"], "pkg/gui-installer/default-admin.py")

            if os.path.exists(default_admin_script_path):
                logger.warning("Creating development admin config")
                p_args = [
                    default_admin_script_path, "-e", "support@xcalar.com",
                    "-u", "admin", "-p", "admin"
                ]

                os.makedirs(os.path.dirname(admin_json_path))
                with open(admin_json_path, "w") as f:
                    subprocess.run(p_args, stdout=f)
                os.chmod(admin_json_path, stat.S_IRUSR | stat.S_IWUSR)

        # Make our temp directory
        log_dir = tempfile.mkdtemp(prefix="xce")
        # Make a symlink from /tmp/xce to the latest launch output
        symlink_path = "/tmp/xce-{}".format(os.getuid())
        try:
            os.remove(symlink_path)
        except FileNotFoundError:
            pass
        os.symlink(log_dir, symlink_path)

        self._set_license_env_vars()

        self._launch_xce(log_dir)

        logger.info("XCE launched; awaiting up status")

        time_waited = 0
        while time_waited < timeout:
            if self.is_up():
                break
            wait_time = 1.0
            time.sleep(wait_time)
            time_waited += wait_time
        else:
            raise RuntimeError("timed out waiting for cluster to start")

        logger.info("XCE up after {:.2f}s".format(time_waited))

    def kill_wait(self, pid, sig, timeout):
        waited = 0
        wait_time = 0.1
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            return True
        logger.debug("XCE kill_wait PID:{}, signal:{}, timeout:{}s".format(
            pid, sig, timeout))

        while waited < timeout:
            try:
                # Try waitpid in case it's our child in order to avoid a zombie
                ret = os.waitpid(pid, os.WNOHANG)
                if (ret[0] == pid):
                    return True
            except (ChildProcessError, ProcessLookupError) as e:
                try:
                    # See if the process is still alive
                    os.kill(pid, 0)
                except OSError:
                    return True

            time.sleep(wait_time)
            waited += wait_time

        return False

    def stop(self,
             term_timeout=DEF_CLUSTER_TERM_TIMEOUT,
             kill_timeout=DEF_CLUSTER_KILL_TIMEOUT):
        #  The key is to first use SIGTERM for graceful killing of xcmon-0
        #  (which should bring down all xcmon/usrnode pairs) and other misc
        #  procs.
        #
        #  If SIGTERM isn't successful within term_timeout secs, use SIGKILL.
        #  If SIGTERM is successful in killing xcmon-0, then wait for 30 secs
        #  for other xcmon/usrnode pairs to die.
        #
        #  Detailed logic:
        #
        #  - first send SIGTERM to xcmonitor-0 and all misc procs (other than
        #    the xcmon/usrnode pairs)
        #
        #  - if SIGTERM to xcmon-0 isn't successful within term_timeout, set
        #    the xcmon_term_fail flag, else wait for 30 secs for other
        #    xcmon/usrnode pairs to die
        #
        #  - if SIGTERM to a misc proc isn't successful, use SIGKILL - if this
        #    isn't successful within kill_timeout, fail the command
        #
        #  - if xcmont_term_fail flag is set, use SIGKILL on all xce procs
        #
        #  - if latter fails, fail the command
        logger.debug("term_timeout: {} kill_timeout: {}".format(
            term_timeout, kill_timeout))
        xcmonitor_term_fail = False
        for proc in _xce_processes():
            proc_name = proc.get("name")
            proc_pid = proc.get("pid")
            if term_timeout != 0:
                if proc_name == "xcmonitor 0":
                    ret = self.kill_wait(proc_pid, signal.SIGTERM,
                                         term_timeout)
                    if not ret:
                        logger.warning(
                            "Failed to SIGTERM process {} pid {}, falling back to SIGKILL"
                            .format(proc["name"], proc_pid))
                        xcmonitor_term_fail = True
                elif not proc_name.startswith(
                        "usrnode") and not proc_name.startswith("xcmonitor"):
                    ret = self.kill_wait(proc_pid, signal.SIGTERM,
                                         term_timeout)
                    if not ret:
                        logger.warning(
                            "Failed to SIGTERM process {} pid {}, falling back to SIGKILL"
                            .format(proc_name, proc_pid))
                    ret = self.kill_wait(proc_pid, signal.SIGKILL,
                                         kill_timeout)
                    if not ret:
                        raise RuntimeError(
                            "timed out waiting for cluster to stop")
            else:
                ret = self.kill_wait(proc_pid, signal.SIGKILL, kill_timeout)
                if not ret:
                    raise RuntimeError("timed out waiting for cluster to stop")
        waited = 0
        wait_time = 0.1
        wait_for_term = 30
        while not xcmonitor_term_fail and waited < wait_for_term:
            if self.is_fully_down():
                break
            time.sleep(wait_time)
            waited += wait_time
        # if the term of xcmonitor 0 fail to kill all usrnode and zombies try to kill mannually
        if not self.is_fully_down():
            for proc in _xce_processes():
                while (_is_process_core_dumping(proc["pid"])):
                    logger.info(
                        "{} (pid {}) is core-dumping. Let's wait".format(
                            proc["name"], proc["pid"]))
                    time.sleep(10)
                self.kill_wait(proc["pid"], signal.SIGKILL, kill_timeout)
        if not self.is_fully_down():
            for proc in _xce_processes():
                logger.error("{} (pid {}) still up {}s after SIGKILL".format(
                    proc["name"], proc["pid"], term_timeout))
            raise RuntimeError("timed out waiting for cluster to stop")

        if self._guardrailsArgs:
            grdump = False
            for fname in os.listdir('.'):
                if fnmatch.fnmatch(fname, 'grtrack-*.txt'):
                    grdump = True
                    logger.info("\nDump from {}".format(fname))
                    grDumper = os.environ.get(
                        "XLRINFRADIR"
                    ) + "/GuardRails/grdump.py" + " -f {} -b {} -t 100".format(
                        fname, "$(which usrnode)")
                    os.system(grDumper)
            if grdump:
                logger.info("\nGuardrails dumps complete!\n")

        _check_for_cores()

    def status(self):
        """Get the current status of the cluster

        Returns a dict with at least the following fields:
        {
            "xcmonitor 0": { # for a running process
                "name": "xcmonitor 0",
                "status": "started",
                "pid": "1234",
                "uptime": "12:00",
            },
            "xcmgmtd": { # for a not-running process
                "name": "xcmgmtd",
                "status": "down",
                "pid": "",
                "uptime": "",
            },
        }"""
        statuses = []
        procs = list(_xce_processes())
        proc_table = {p["name"]: p for p in procs}
        exp_procs = self._expected_processes()

        for exp_p in exp_procs:
            if exp_p not in proc_table:
                proc_info = {}
                proc_info["name"] = exp_p
                proc_info["status"] = ProcessState.DOWN
                proc_info["pid"] = ""
                proc_info["uptime"] = ""
                proc_table[exp_p] = proc_info

        # We have basic process information; now let's check health
        for proc in proc_table.values():
            # if a process doesn't have a health command,
            # we can just upgrade it from 'started' to 'up' optimistically
            if (proc["name"] in ["xcmgmtd"]
                    or proc["name"].startswith("xcmonitor")):
                if proc["status"] == ProcessState.STARTED:
                    proc["status"] = ProcessState.UP

        # XXX we only check node 0 and apply that to all usrnodes
        if proc_table["usrnode 0"]["status"] == ProcessState.STARTED:
            node_up = subprocess.run(["xccli", "-c", "version"],
                                     stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL).returncode == 0

            for proc in proc_table.values():
                if proc["name"].startswith("usrnode"):
                    proc["status"] = ProcessState.UP if node_up else proc[
                        "status"]

        # Check if expServer, sqldf are responding to requests yet
        # XXX hardcoded
        proc_url_mapping = {
            "expServer": "http://localhost:12124/service/status",
            "sqldf": "http://localhost:27000/xcesql/info"
        }
        for proc, url in proc_url_mapping.items():
            if proc_table[proc]["status"] == ProcessState.STARTED:
                proc_up = False
                # requests will dump out a bunch of logs; let's disable those
                requests_logger = logging.getLogger("urllib3")
                old_level = requests_logger.getEffectiveLevel()
                requests_logger.setLevel(logging.WARNING)
                try:
                    resp = requests.get(url)
                    # Reject anything other than HTTP OK
                    proc_up = resp.status_code == 200
                except requests.exceptions.ConnectionError as e:
                    pass
                finally:
                    requests_logger.setLevel(old_level)

                proc_table[proc][
                    "status"] = ProcessState.UP if proc_up else ProcessState.STARTED
        return proc_table

    def is_up(self):
        """Check if the Xcalar cluster is up; returns a bool"""
        # First check if all requiried processes are running
        # "xcmonitor" (no node number) is a zombie; never require it
        optional_procs = ["xcmonitor"]
        status = self.status()
        for proc in status.values():
            proc_name = proc["name"]
            proc_status = proc["status"]

            logger.info("is_up proc {} status {}".format(
                proc_name, proc_status))

            if proc_name in optional_procs:
                logger.info("is_up skip optional")
                continue

            if proc_status != ProcessState.UP:
                logger.info("is_up returns False")
                return False

        logger.info("is_up returns True")
        return True

    def is_fully_down(self):
        """Check if the Xcalar cluster is completely stopped; returns a bool"""
        # First check if all requiried processes are running
        status = self.status()
        for proc in status.values():
            if proc["status"] != ProcessState.DOWN:
                return False
        return True

    @property
    def config(self):
        return self._config

    def get_xcalar_api(self):
        # This is localhost; so this should work by default
        return XcalarApi()

    def client(self, *args, **kwargs):
        # This is localhost; so this should work by default
        if "bypass_proxy" not in kwargs:
            kwargs["bypass_proxy"] = True
        return Client(*args, **kwargs)

    def _launch_xce(self, log_dir):
        """Kick off all Xcalar cluster processes; does not wait for them"""
        if xc2debug:
            ioBenchProc = self._dbg_iobenchmark(log_dir, 128)
            # First run IO benchmark synchronously in isolation
            ioBenchProc.wait()

        self._start_xcmgmtd(log_dir)
        self._start_exp_server(log_dir, 0)

        if xc2debug:
            # Rerun IO benchmark asynch concurrent with usrnode and sqldf load
            ioBenchProc = self._dbg_iobenchmark(log_dir, 128)

        # sqldf is allowed to fail to start
        try:
            self._start_sqldf(log_dir)
        except RuntimeError as e:
            logger.warning("Failed to start sqldf: {}".format(e))

        for node_id in range(self._num_nodes):
            self._start_xcmonitor(node_id, log_dir)

        if xc2debug:
            ioBenchProc.wait()

    def _set_asan_env_vars(self):
        """Setup ASAN environment"""
        conf = self._config.config_file_path
        vaConf = [
            re.findall(r'Constants.EnforceVALimit=false', l)
            for l in open(conf)
        ]
        if len(vaConf) == 0:
            raise RuntimeError(
                "Config file {} must set Constants.EnforceVALimit=false".
                format(conf))

        env = os.environ.copy()
        xlr_dir = os.environ["XLRDIR"]
        asan_supp = "{}/bin/ASan.supp".format(xlr_dir)
        lsan_supp = "{}/bin/LSan.supp".format(xlr_dir)
        ubsan_supp = "{}/bin/UBSan.supp".format(xlr_dir)
        env["ASAN_OPTIONS"] = "strict_string_checks=true:verbosity=1:allow_addr2line=true:allocator_may_return_null=1:suppressions={}".format(
            asan_supp)
        env["LSAN_OPTIONS"] = "suppressions={}".format(lsan_supp)
        env["UBSAN_OPTIONS"] = "print_stacktrace=1:suppressions={}".format(
            ubsan_supp)
        return env

    def _set_cgroup_env_vars(self, env=None):
        if env is None:
            env = os.environ.copy()

        # if we're not using cgroups, do nothing
        if not self._use_cgroups:
            return env

        child_node_paths = ':'.join(
            set(
                os.path.join(val, self.usrnode_unit_path)
                for val in self.cgroup_path_dict.values()))
        # child_node_paths and XCE_CHILDNODE_PATHS should be the same, but because of cgroup
        # config problems on our jenkins slaves, cpuset controllers are not properly set up
        # TODO fix cpuset controllers
        env["XCE_CHILDNODE_PATHS"] = ':'.join(
            set(
                os.path.join(val, self.usrnode_unit_path)
                for val in self.cgroup_path_dict.values()
                if 'cpuset' not in val))
        env["XCE_CGROUP_CONTROLLER_MAP"] = ':'.join(
            set(key + '%' + val for key, val in self.cgroup_path_dict.items()))
        env["XCE_CGROUP_CONTROLLERS"] = "memory cpu cpuacct cpuset"
        env["XCE_CGROUP_UNIT_PATH"] = self.usrnode_unit_path
        env["XCE_CHILDNODE_SCOPES"] = "sys_xpus-sched0 sys_xpus-sched1 sys_xpus-sched2 usr_xpus-sched0 usr_xpus-sched1 usr_xpus-sched2"
        env["XCE_USRNODE_SCOPE"] = "xcalar-usrnode.scope"

        my_uid = os.geteuid()
        my_gid = os.getgid()
        for child_path in child_node_paths.split(':'):
            if (os.stat(child_path).st_uid != my_uid):
                cmd = 'sudo chown -R {}:{} {} '.format(my_uid, my_gid,
                                                       child_path)
                pr = subprocess.Popen(
                    cmd,
                    stderr=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    shell=True)
                (output, err) = pr.communicate()
                p_status = pr.wait()
                if (p_status != 0):
                    print("Error during chown of {}: {}".format(child_path))

        return env

    def _set_license_env_vars(self):
        """Setup license environment variables"""
        if "XCE_LICENSEDIR" not in os.environ:
            xlr_dir = os.environ["XLRDIR"]
            os.environ["XCE_LICENSEDIR"] = os.path.join(xlr_dir, "src/data")

        if "XCE_LICENSEFILE" not in os.environ:
            lic_file = os.path.join(os.environ["XCE_LICENSEDIR"],
                                    "XcalarLic.key")
            os.environ["XCE_LICENSEFILE"] = lic_file

    def _cgexec_wrap(self, cgroup_name, *args):
        if self._use_cgroups:
            cmd_list = [
                "cgexec", "-g", "cpu,cpuacct,memory:{}".format(cgroup_name),
                "--sticky"
            ]
        else:
            cmd_list = []
        return cmd_list + list(args)

    def _start_exp_server(self, log_dir, exp_id=0):
        exp_server_path = _find_exp_server_path()
        env = os.environ.copy()
        env["XCE_CONFIG"] = self._config.config_file_path
        env["XCE_EXP_ID"] = str(exp_id)
        cmd = self._cgexec_wrap(
            "xcalar_middleware_{}".format(os.environ['USER']), "npm", "start",
            "--prefix", exp_server_path)
        with open(os.path.join(log_dir, "expServer.out"), "a") as outfile:
            pr = subprocess.Popen(
                cmd, stderr=subprocess.STDOUT, stdout=outfile, env=env)
            logger.info("ExpServer pid {}".format(pr.pid))

    def _setupGuardRails(self):
        conf = self._config.config_file_path
        ldpConf = [
            re.findall(r'Constants.NoChildLDPreload=true', l)
            for l in open(conf)
        ]
        if len(ldpConf) == 0:
            raise RuntimeError(
                "Config file {} must set Constants.NoChildLDPreload=true".
                format(conf))

        with open('/proc/sys/vm/max_map_count', 'r') as fh:
            vmMapMax = int(fh.readline())

        if vmMapMax < 1000000:
            errStr = "GuardRails VM max map value {} too low, please increase".format(
                vmMapMax)
            logger.error(errStr)
            logger.error(
                "     eg: echo 10000000 | sudo tee /proc/sys/vm/max_map_count")
            raise RuntimeError(errStr)

        grPath = os.environ.get("GUARD_RAILS_PATH")
        if not grPath:
            grPath = os.environ.get(
                "XLRINFRADIR") + "/GuardRails/libguardrails.so.0.0"

        if not grPath:
            raise FileNotFoundError("GuardRails path not set")

        if not os.path.isfile(grPath):
            raise FileNotFoundError("GuardRails binary not found at " + grPath)

        # Write out guardrails config
        with open('grargs.txt', 'w') as fh:
            fh.write(self._guardrailsArgs)

        for file in os.listdir('.'):
            if fnmatch.fnmatch(file, 'grtrack-*.txt'):
                logger.info("Remove '{}' guardrails tracker file".format(file))
                os.remove(file)

        return (grPath)

    def _start_xcmonitor(self, node_id, log_dir):
        out_fn = "xcmonitor.{}".format(node_id)
        cmd = [
            "xcmonitor",
            "-n",
            str(node_id),
            "-m",
            str(self._num_nodes),
            "-c",
            self._config.config_file_path,
        # XXX this might not be correct ?
            "-k",
            os.environ.get("XCE_LICENSEFILE")
        ]

        if self._use_cgroups:
            cgroupControllers = list(
                set(
                    os.path.join(val, self.usrnode_unit_path,
                                 'xcalar-usrnode.scope')
                    for (key, val) in self.cgroup_path_dict.items()
                    if key in ['memory', 'cpu', 'cpuacct']))

        if self._guardrailsArgs:
            try:
                grPath = self._setupGuardRails()
                cmd += ["-g", grPath]
                logger.info("Running with GuardRails from " + grPath)
            except Exception as e:
                logger.warning(
                    "Unable to set up guardrails, continuing without: " +
                    str(e))

        env = self._set_asan_env_vars()
        env = self._set_cgroup_env_vars(env)

        with open(os.path.join(log_dir, out_fn), "a") as outfile:
            pr = subprocess.Popen(
                cmd, stderr=subprocess.STDOUT, stdout=outfile, env=env)
            logger.info("xcmonitor {} pid {}".format(node_id, pr.pid))
            if self._use_cgroups:
                # we go through the sleep and finding the usrnode child process
                # because adding the xcmonitor without the delay intermittently
                # failed -- worked for cpuset, failed for memory and cpuacct
                time.sleep(2)
                parent = psutil.Process(pr.pid)
                children = parent.children()
                xcm_pids = [pr.pid]
                if (len(children) > 0):
                    # xcmonitor currently has one child: usrnode
                    xcm_pids.append(children[0].pid)
                for controller in cgroupControllers:
                    for xcm_pid in xcm_pids:
                        with open(
                                os.path.join(controller, "cgroup.procs"),
                                "a") as procfile:
                            procfile.write("{}\n".format(xcm_pid))
                            procfile.flush()

    def _start_xcmgmtd(self, log_dir):
        cmd = self._cgexec_wrap(
            "xcalar_middleware_{}".format(os.environ['USER']), "xcmgmtd",
            self._config.config_file_path)
        with open(os.path.join(log_dir, "xcmgmtd.out"), "a") as outfile:
            pr = subprocess.Popen(
                cmd, stderr=subprocess.STDOUT, stdout=outfile)
            logger.info("xcmgmtd pid {}".format(pr.pid))

    def _start_sqldf(self, log_dir):
        sqldf_path = _find_sqldf_path()
        cmd = self._cgexec_wrap(
            "xcalar_middleware_{}".format(os.environ['USER']), "java", "-jar",
            sqldf_path, '-jPn', '-R', str(10000))
        with open(os.path.join(log_dir, "sqldf.out"), "a") as outfile:
            try:
                pr = subprocess.Popen(
                    cmd, stderr=subprocess.STDOUT, stdout=outfile)
                logger.info("sqldf pid {}".format(pr.pid))
            except FileNotFoundError as e:
                logger.warning("unable to start sqldf; java not found")
                # allow this to proceed

    def _start_jupyterd(self, log_dir):
        env = os.environ.copy()
        _set_jupyter_config_path()
        env["PYTHONUSERBASE"] = os.path.join(env["HOME"], ".local")
        env["XCE_CONFIG"] = self._config.config_file_path
        jupyter_binary = "jupyter-notebook"
        jupyter_bin_path = shutil.which(jupyter_binary)
        if not jupyter_bin_path:
            jupyter_bin_path = jupyter_binary
        cmd = self._cgexec_wrap(
            "xcalar_middleware_{}".format(os.environ['USER']), "python",
            jupyter_bin_path)
        with open(os.path.join(log_dir, "jupyter.out"), "a") as outfile:
            pr = subprocess.Popen(
                cmd, stderr=subprocess.STDOUT, stdout=outfile, env=env)
            logger.info("jupyterd pid {}".format(pr.pid))

    def _expected_processes(self):
        """Produce all expected xcalar processes for this cluster"""
        expected = []
        for node_id in range(self._num_nodes):
            expected.append("xcmonitor {}".format(node_id))
            expected.append("usrnode {}".format(node_id))
        expected.append("xcmonitor")
        expected.append("expServer")
        expected.append("xcmgmtd")
        expected.append("sqldf")
        return expected

    def _dbg_iobenchmark(self, log_dir, dataMB):
        blockSize = 4096
        blocks = int(dataMB * 1024 * 1024 / 4096)
        logger.info(
            "Commence debug IO benchmark {} MB ({} {} byte blocks)".format(
                dataMB, blocks, blockSize))

        shCmd = '''
        usrnode="$(which usrnode)"
        usrnodedir="$(dirname $usrnode)" # Make sure to get the mountpoint serving usrnode
        ofFile="$usrnodedir/ddtest.bin"
        logFile="{}/iobenchmark_$(date +%s%3N).txt"
        dd if=/dev/urandom of=$ofFile bs={} count={} 2>&1 |tee $logFile
        (time md5sum $ofFile) 2>&1 |tee -a $logFile
        rm "$ofFile"
        '''

        pr = subprocess.Popen(
            ["/bin/sh", "-c",
             shCmd.format(log_dir, blockSize, blocks)])

        return pr
