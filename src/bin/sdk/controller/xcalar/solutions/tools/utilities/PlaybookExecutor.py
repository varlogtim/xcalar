import os
import json
import tempfile
import shutil
try:
    from ansible.module_utils.common.collections import ImmutableDict
    from ansible.parsing.dataloader import DataLoader
    from ansible.vars.manager import VariableManager
    from ansible.inventory.manager import InventoryManager
    from ansible.playbook.play import Play
    from ansible.executor.task_queue_manager import TaskQueueManager
    from ansible import context
    from ansible.plugins.callback import CallbackBase
    import ansible.constants as C
    HAVE_ANSIBLE = True
except ImportError:
    HAVE_ANSIBLE = False
from xcalar.solutions.tools.utilities.nodes import get_nodes_list

if HAVE_ANSIBLE:
    class ResultCallback(CallbackBase):
        """A sample callback plugin used for performing an action as results come in

        If you want to collect all results into a single object for processing at
        the end of the execution, look into utilizing the ``json`` callback plugin
        or writing your own custom callback plugin
        """

        def v2_runner_on_ok(self, result, **kwargs):
            """Print a json representation of the result

            This method could store the result in an instance attribute for retrieval later
            """
            host = result._host
            print(json.dumps({host.name: result._result}, indent=4))
else:
    class ResultCallback:
        def v2_runner_on_ok(self, result, **kwargs):
            pass


class XcalarPlaybookExecutor():
    def __init__(
            self,
            play_source,
            connection='ssh',
            module_path=None,
            forks=25,
            remote_user='xcalar',
            private_key_file=None,
            ssh_common_args=None,
            ssh_extra_args=None,
            sftp_extra_args=None,
            scp_extra_args=None,
            become=True,
            become_method='sudo',
            become_user='root',
            verbosity=20,
            check=False,
            vault_pass=None,
            inventory_file_list=None,
    ):
        self.files = []

        self.play_source = play_source
        self.connection = connection
        self.module_path = module_path
        self.forks = forks
        self.remote_user = remote_user
        self.private_key_file = private_key_file
        self.ssh_common_args = ssh_common_args
        self.ssh_extra_args = ssh_extra_args
        self.sftp_extra_args = sftp_extra_args
        self.scp_extra_args = scp_extra_args
        self.become = become
        self.become_method = become_method
        self.become_user = become_user
        self.verbosity = verbosity
        self.check = check

        if not HAVE_ANSIBLE:
            return

        self.loader = DataLoader()
        self.passwords = dict(vault_pass=vault_pass)
        self.inventory_file_list = inventory_file_list if inventory_file_list else self.get_temp_inventory_file(
        )
        self.inventory = InventoryManager(
            loader=self.loader, sources=self.inventory_file_list)

        self.variable_manager = VariableManager(
            loader=self.loader, inventory=self.inventory)
        self.context = context
        self.results_callback = ResultCallback()
        self.setup()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for file in self.files:
            os.unlink(file)

    def setup(self):
        if not HAVE_ANSIBLE:
            return

        self.context.CLIARGS = ImmutableDict(
            connection=self.connection,
            module_path=self.module_path,
            forks=self.forks,
            remote_user=self.remote_user,
            private_key_file=self.private_key_file,
            ssh_common_args=self.ssh_common_args,
            ssh_extra_args=self.ssh_extra_args,
            sftp_extra_args=self.sftp_extra_args,
            scp_extra_args=self.scp_extra_args,
            become=self.become,
            become_method=self.become_method,
            become_user=self.become_user,
            verbosity=self.verbosity,
            check=self.check,
        )

        self.play = Play().load(
            self.play_source,
            variable_manager=self.variable_manager,
            loader=self.loader)

    def get_pretty_print(self, json_object):
        return json.dumps(
            json_object, sort_keys=True, indent=4, separators=(',', ': '))

    def get_temp_inventory_file(self):
        # temp inventory file
        self.tmp = tempfile.NamedTemporaryFile(mode='w+t')
        inventory_list = get_nodes_list()
        for host in inventory_list:
            self.tmp.writelines(f'{host}\n')
        self.tmp.seek(0)
        self.files.append(self.tmp.name)
        print(self.tmp.name)
        return [self.tmp.name]

    def run(self):
        if not HAVE_ANSIBLE:
            return

        os.environ['ANSIBLE_PYTHON_INTERPRETER'] = 'auto_silent'
        os.environ['ANSIBLE_HOST_KEY_CHECKING'] = 'false'
        os.environ['ANSIBLE_FORCE_COLOR'] = 'true'
        os.environ['ANSIBLE_ACTION_WARNINGS'] = 'false'
        os.environ['ANSIBLE_COMMAND_WARNINGS'] = 'false'
        os.environ['ANSIBLE_BECOME_ASK_PASS'] = 'false'

        tqm = None
        try:
            tqm = TaskQueueManager(
                inventory=self.inventory,
                variable_manager=self.variable_manager,
                loader=self.loader,
                passwords=self.passwords,
            #   stdout_callback=self.results_callback,  # Use our custom callback instead of the ``default`` callback plugin, which prints to stdout
            )
            result = tqm.run(self.play)
        finally:
            if tqm is not None:
                tqm.cleanup()

            shutil.rmtree(C.DEFAULT_LOCAL_TMP, True)


if __name__ == '__main__':
    play_source = dict(
        name='Host Name',
        hosts='all',
        gather_facts='no',
        tasks=[
    # dict(action=dict(module='shell', args='systemctl status indy-node'), register='shell_out'),
            dict(
                action=dict(module='shell', args='uptime'),
                register='shell_out'),
            dict(
                action=dict(
                    module='debug', args=dict(msg='{{shell_out.stdout}}')))
        ])

    # inventory_file_list = ['/Users/francis/dev-work/ansible_remote_management/hosts.ini']
    executor = XcalarPlaybookExecutor(
        play_source=play_source,
        remote_user='xcalar',
    # inventory_file_list= inventory_list
        private_key_file='/home/xcalar/.ssh/id_rsa',
        inventory_file_list='10.20.3.87,')
    executor.run()
