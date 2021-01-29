import os
import configparser
import xcalar.compute.util.config as x_config
import pkg_resources
import subprocess
import sys

# XCE_LOGDIR
# XCE_LOGDIR
# XCE_USER_HOME

def required_module(pysite=None):
    required = {'colorama', 'ansible', 'confluent-kafka', 'fastavro'}
    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = required - installed
    if missing:
        print('INFO: install missing modules ... please wait')
        python = sys.executable
        # /opt/xcalar/bin/python3 -m pip install -t {site_packages} {packages}
        subprocess.check_call(
            ['/opt/xcalar/bin/python3', '-m', 'pip', 'install', '-t', f'{pysite}',  *missing],
            stdout=subprocess.DEVNULL
        )

def get_xcalar_properties(XCALAR_PATH='/opt/xcalar/etc/default/xcalar'):
    property = {}
    # because there is no section in the file. We create a fake one 'xcalar' to satisfy configparser requirements

    if os.path.exists(XCALAR_PATH):

        # ----------
        # xcalar
        # ----------
        with open(XCALAR_PATH, 'r') as f:
            config_string = '[xcalar]\n' + f.read()

        config = configparser.ConfigParser( strict=False, comment_prefixes=('//', '#'))
        config.read_string(config_string)
        property = {key.upper(): value for (key, value) in config['xcalar'].items()}

        # -------------
        # default.cfg
        # -------------
        DEFAULT_CONFIG_PATH = config['xcalar']['XCE_CONFIG']
        try:
            with open(DEFAULT_CONFIG_PATH, 'r') as f:
                config_string = '[xcalar]\n' + f.read()
        except Exception as e:
            print(f'Info: {e}')

        config = configparser.ConfigParser( strict=False, comment_prefixes=('//', '#'))
        config.read_string(config_string.replace('//', '#'))
        property[
            'PYSITE'
        ] = f"""{config['xcalar']['Constants.XcalarRootCompletePath']}/pysite"""

    else:
        try:
            config = x_config.detect_config()
            property['XCE_CONFIG'] = config.config_file_path
        except Exception as e:
            print(f'Info: {e},use default settings.')
            property['XCE_CONFIG'] = ''
            pass

        HOME = os.environ['HOME']
        if 'XCALAR_AUDIT_CONFIG' in os.environ:
            property['XCE_LOGDIR'] = os.environ['XCALAR_AUDIT_CONFIG']
        else:
            property['XCE_LOGDIR'] = f'{HOME}/.xcalar/log/'

        property['XCE_USER'] = 'xcalar'
        property['XCE_USER_HOME'] = HOME
        property['PYSITE'] = f'{HOME}/.xcalar/pysite'

    required_module(pysite=property['PYSITE'])
    return property
