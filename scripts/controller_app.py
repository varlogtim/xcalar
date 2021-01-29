from xcalar.solutions.controller import Controller
import logging
import json
from datetime import datetime
from xcalar.container.cluster import get_running_cluster
import xcalar.container.context as ctx
import sys
import os
from contextlib import contextmanager
import tempfile

temp_file = tempfile.gettempdir()
# Set up logging
logger = logging.getLogger('xcalar')
logger.setLevel(logging.INFO)

# This module gets loaded multiple times. The global variables do not
# get cleared between loads. We need something in place to prevent
# multiple handlers from being added to the logger.
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stderr
    log_handler = logging.StreamHandler(sys.stderr)
    this_xpu_id = ctx.get_xpu_id()
    this_node_id = ctx.get_node_id(this_xpu_id)
    formatter = logging.Formatter(
        '%(asctime)s - Pid {} Node {} - Sample Controller App - %(levelname)s - %(message)s'
        .format(os.getpid(), this_node_id))
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)


def startController(universeId, username, properties, runAsApp=True):
    try:
        controller = Controller()
        logger.info('get controller')
        controller.initialize(
            universeId,
            properties=properties,
            sessionName=universeId,
            runAsApp=runAsApp,
            username=username)
        controller.eventloop()
    except Exception as e:
        logger.info(e)


@contextmanager
def pidfile(path):
    logger.info(path)
    try:
        with open(path, 'w') as fp:
            yield fp
    finally:
        try:
            os.remove(path)
        except IOError:
            sys.stderr.write('Failed to delete controller_app.pid')


def main(inBlob):
    logger.info(
        f'controller_app: I am called with args:[ {inBlob} ] at {datetime.now()}'
    )
    params = json.loads(inBlob)
    cluster = get_running_cluster()
    my_node_id = cluster.my_node_id
    master_node_id = ctx.get_node_id(cluster.master_xpu_id)

    is_local_master = True

    if my_node_id != master_node_id:
        is_local_master = cluster.is_local_master()
    else:
        is_local_master = cluster.is_master()

    if is_local_master:
        path = temp_file + '/' + params['universe_id'] + '_controller_app.pid'
        with pidfile(path) as fp:
            fp.write('{}'.format(os.getpid()))
            fp.flush()
            pythonPath = params['python_file_path']
            sys.path.append(pythonPath)
            properties = {}
            properties['git'] = params['git']
            properties['callbacksPluginName'] = params['callbacksPluginName']
            properties['universeAdapterPluginName'] = params['universeAdapterPluginName']
            logger.info(f'{properties}')
            startController(params['universe_id'], params['username'],
                            properties
                            )
