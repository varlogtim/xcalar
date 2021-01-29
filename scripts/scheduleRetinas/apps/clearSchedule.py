import subprocess
import json
import traceback
import logging
import sys
from socket import gethostname


# Dynamic Constants
hostname = gethostname()

# Set up logging
logger = logging.getLogger('Clear Schedule App Logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    logHandler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
            '%(asctime)s - Clear Schedule App - %(levelname)s - %(message)s')
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("Clear Schedule app initialized; hostname:{}".format(gethostname()))

def removeCronTabOnStartup():
    cmd = "crontab -r"
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    p.wait()

def main(inBlob):
    logger.info("Received input: {}".format(inBlob))
    try:
        removeCronTabOnStartup()
        return json.dumps({})
    except Exception as e:
        traceback = traceback.format_exc()
        print({"status": -1, "error": e, "traceback": traceback})
