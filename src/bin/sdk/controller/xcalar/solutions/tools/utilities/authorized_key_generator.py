import os
import subprocess
import distutils.util

from xcalar.solutions.tools.utilities.nodes import get_nodes_list
from xcalar.solutions.tools.utilities.xcalar_config import get_xcalar_properties
from xcalar.solutions.tools.utilities.PlaybookExecutor import XcalarPlaybookExecutor


def get_aggrement(question, default='no'):
    if default is None:
        prompt = " [y/n] "
    elif default == 'yes':
        prompt = " [Y/n] "
    elif default == 'no':
        prompt = " [y/N] "
    else:
        raise ValueError(f"Unknown setting '{default}' for default.")

    while True:
        try:
            resp = input(question + prompt).strip().lower()
            if default is not None and resp == '':
                return True
            else:
                return bool(distutils.util.strtobool(resp))
        except ValueError:
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def create_authorized_key(config_path='/opt/xcalar/etc/default/xcalar'):
    if os.path.exists(config_path):
        properties = get_xcalar_properties(config_path)
        user_home = properties['XCE_USER_HOME']
    else:
        user_home = '/home/xcalar'

    if os.path.exists(f'{user_home}/.ssh/authorized_keys'):
        return

    if get_aggrement(
            'Authroized keys not found, Do you want to create authorized keys ?',
    ) is False:
        return

    command = f'''
    mkdir -p {user_home}/.ssh;
    chmod 0700 {user_home}/.ssh;
    touch {user_home}/.ssh/authorized_keys;
    chmod 0600 {user_home}/.ssh/authorized_keys;
    ssh-keygen -N '' -b 2048 -f {user_home}/.ssh/id_rsa;   # only on node0!
    cat {user_home}/.ssh/id_rsa.pub >> {user_home}/.ssh/authorized_keys # all nodes
    '''

    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    proc_stdout = process.communicate()[0].strip()
    print(proc_stdout)

    # deploy
    deploy_authorized_key(user_home)


def deploy_authorized_key(user_home='/home/xcalar'):
    hosts_list = get_nodes_list()
    if not hosts_list:
        hosts_list = ['127.0.0.1']
    hosts_string = ','.join(hosts_list)

    # read authorized_key from {user_home}/.ssh/authorized_keys
    authorized_key = None
    with open(f'{user_home}/.ssh/authorized_keys') as fh:
        for last_line in fh:
            pass
        authorized_key = last_line

    play_source = dict(
        name=f'deploy authorized key',
        hosts='all',
        tasks=[
            dict(
                action=dict(
                    module='file',
                    args=dict(
                        name='make directory',
                        path=f'{user_home}/.ssh',
                        state='directory'))),
            dict(
                action=dict(
                    module='file',
                    args=dict(
                        name='create empty file',
                        path=f'{user_home}/.ssh/authorized_keys',
                        state='touch'))),
            dict(
                action=dict(
                    module='lineinfile',
                    args=dict(
                        name='put key',
                        path=f'{user_home}/.ssh/authorized_keys',
                        line=f'{authorized_key}')))
        ])

    # ---------------------
    # executor
    # ---------------------
    executor = XcalarPlaybookExecutor(
        play_source=play_source,
        remote_user='xcalar',
        private_key_file='{user_home}/.ssh/id_rsa',
        inventory_file_list=f'{hosts_string},')
    executor.run()


if __name__ == '__main__':
    create_authorized_key()
