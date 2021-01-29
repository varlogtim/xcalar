import json
import os
from pathlib import Path
from xcalar.external.app import App
import tempfile


def get_key(topic):
    return f'kafka_app_{topic}::group_id'


def get_group_id(kvstore, topic):
    key = get_key(topic)
    if len(kvstore.list(key)) == 0:
        return None
    return kvstore.lookup(key)


def is_app_alive(client, topic):
    group_id = get_group_id(client.global_kvstore(), topic)
    if group_id is None:
        return False
    app = App(client)
    return app.is_app_alive(group_id)


def get_pidfile_path(topic):
    return f'{tempfile.gettempdir()}/kafka_{topic}_listener_app.pid'


def runKafkaApp(appName, args, client, async_mode=False):
    print(f'Running {appName} with {args}, async_mode: {async_mode}')
    print(f"Topic: {args['configuration']['kafka_config']['topic']}")

    args['async_mode'] = async_mode
    topic = args['configuration']['kafka_config']['topic']
    app = App(client)
    app_code = os.path.dirname(Path(__file__)) + '/kafka_app.py'
    code = ''
    with open(app_code) as fp:
        code = '\n'.join(fp.readlines())
    app.set_py_app(appName, code)
    if async_mode:
        key = args["alias"] if "alias" in args else topic
        if is_app_alive(client, key):
            raise Exception('App is already running.')
        group_id = app.run_py_app_async(appName, False, json.dumps(args))
        kvstore = client.global_kvstore()
        kvstore.add_or_replace(get_key(key), group_id, True)
        return group_id
    else:
        (outStr, errStr) = app.run_py_app(appName, False, json.dumps(args))
        return (outStr, errStr)


def runKafkaAppFromShell(appName,
                         configuration,
                         session_name,
                         xcalar_username,
                         xcalar_client,
                         num_runs,
                         profile=False,
                         async_mode=False,
                         purge_interval_in_seconds=None, #300
                         purge_cycle_in_seconds=None, #30
                         alias=None):
    # configuration = {'session_name': session_name}
    # with open(kafka_properties_path) as cp:
    #     configuration['kafka_config'] = json.load(cp)
    args = {
        'configuration': configuration,
        'profile': profile,
        'num_runs': num_runs,
        'username': xcalar_username,
        'async_mode': async_mode,
        'purge_interval_in_seconds': purge_interval_in_seconds if purge_interval_in_seconds else configuration['kafka_config'].get('purge_interval_in_seconds',300),
        'purge_cycle_in_seconds': purge_cycle_in_seconds if purge_cycle_in_seconds else configuration['kafka_config'].get('purge_cycle_in_seconds',300)
    }
    if alias is not None:
        args['alias'] = alias
    return runKafkaApp(appName, args, xcalar_client, async_mode=async_mode)


def stopKafkaApp(client, topic):
    if is_app_alive(client, topic):
        app = App(client)
        app.cancel(get_group_id(client.global_kvstore(), topic))
        code = '''
import os
import json
def main(inBlob):
    args = json.loads(inBlob)
    os.remove(args["pidfile_path"])
'''
        app.set_py_app('kafka_app_cleanup', code)
        (outStr, errStr) = app.run_py_app(
            'kafka_app_cleanup', False,
            json.dumps({
                'pidfile_path': get_pidfile_path(topic)
            }))
        return (outStr, errStr)
