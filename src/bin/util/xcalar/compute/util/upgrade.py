"""Upgrade on disk Chronos workbooks and dataflows to Dionysus

- Starts expServer and XcUpgradeTool
- Deserialize on disk query graphs to json
- List on disk batch dataflows
- Convert query graphs from workbooks and dataflows to dataflow 2.0 format
- Clean up legacy files
"""

import subprocess
import os
import sys
import logging
import time
import requests
import shutil
import xcalar.compute.util.cluster as cluster
import xcalar.compute.util.config as config


class UpgradeTool:
    def __init__(self, config_path=None, verbose=False, force=False):
        self._config_path = config_path or config._find_config_file()
        self._config = config.build_config(self._config_path)
        self._verbose = "--verbose" if verbose else ""
        self._force = "--force" if force else ""

    def start(self):
        self._start_exp_server()
        ret = self._start_xc_upgrade_tool()

        session_dir = "{}/{}".format(self._config.xcalar_root_path, "sessions")
        shutil.rmtree(session_dir, True)

        if ret == 0:
            logging.warning("Upgrade finished")
        else:
            logging.warning("Upgrade finished. Some files are not upgraded.")
            sys.exit(ret)

    def _start_xc_upgrade_tool(self):
        logging.warning("Starting upgrade tool")
        pr = subprocess.Popen([
            "xcUpgradeTool", "--cfg", self._config_path, self._force,
            self._verbose
        ],
                              stderr=subprocess.STDOUT,
                              stdout=subprocess.PIPE)
        logging.info("xcUpgradeTool pid {}".format(pr.pid))
        for line in iter(pr.stdout.readline, b''):
            line = line.rstrip().decode()
            if line.startswith("xc2 logger "):
                logging.warning(line[11:])
            else:
                print(line)
        pr.communicate()[0]
        return pr.returncode

    def _start_exp_server(self, timeout=15):
        logging.warning("Starting exp server")
        exp_server_path = cluster._find_exp_server_path()
        env = os.environ.copy()
        env["XCE_CONFIG"] = self._config.config_file_path
        pr = subprocess.Popen(["node", exp_server_path],
                              stderr=subprocess.STDOUT,
                              stdout=subprocess.PIPE,
                              env=env)

        time_waited = 0
        while time_waited < timeout:
            # requests will dump out a bunch of logs; let's disable those
            requests_logger = logging.getLogger("urllib3")
            old_level = requests_logger.getEffectiveLevel()
            requests_logger.setLevel(logging.WARNING)
            try:
                # XXX hardcoded
                resp = requests.get("http://localhost:12124/service/status")
                if resp.status_code:
                    break
            except requests.exceptions.ConnectionError:
                pass
            finally:
                requests_logger.setLevel(old_level)

            wait_time = 0.5
            time.sleep(wait_time)
            time_waited += wait_time
        else:
            raise RuntimeError("timed out waiting for exp server to start")

        logging.info("ExpServer pid {}".format(pr.pid))
