import os
import json
import uuid
from pathlib import Path
from tempfile import NamedTemporaryFile


def generatePropertiesFromTemplate():
    template_file = (
        os.path.dirname(Path(__file__)) + '/test_integration_properties.json')
    with open(template_file, 'r') as f:
        props = json.load(f)
        props['xcalar']['url'] = os.environ.get('REMOTE_URL',
                                                props['xcalar']['url'])
        props['xcalar']['username'] = os.environ.get(
            'REMOTE_USERNAME', props['xcalar']['username'])
        props['xcalar']['password'] = os.environ.get(
            'REMOTE_PASSWORD', props['xcalar']['password'])
        props['xcalar']['statsFilePath'] = os.environ.get(
            'STATS_FILE_PATH', '/tmp/stats/')
        props['xcalar']['sessionName'] = os.environ.get(
            'SESSION_NAME', f'test_integration_{uuid.uuid1()}')
        props['git']['url'] = os.environ.get('GITLAB_URL', props['git']['url'])
        props['git']['projectId'] = os.environ.get('GITLAB_PROJECT_ID',
                                                   props['git']['projectId'])
        props['git']['accessToken'] = os.environ.get(
            'GITLAB_ACCESS_TOKEN', props['git']['accessToken'])
        temp_file = NamedTemporaryFile(
            mode='w+',
            prefix='test_integration_1_properties',
            suffix='.json',
            delete=False,
        )
        json.dump(props, temp_file)
        temp_file.flush()
        os.fsync(temp_file.fileno())
        return temp_file.name
