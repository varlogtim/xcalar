import pytest
from IPython.testing.globalipapp import get_ipython

@pytest.fixture(scope='session')
def shell():
    # return ipython shell
    shell = get_ipython()
    shell.magic('load_ext xcalar.solutions.tools.extensions.controller_app_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.sdlc_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.sql_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.systemstat_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.config_editor_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.stats_mart_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.table_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.xdbridge_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.monitor_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.systemctl_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.tmux_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.logmart_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.xpip_ext')
    
    print('@@@@@@ return shell @@@@@')
    yield shell
