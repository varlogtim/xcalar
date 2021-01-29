from xcalar.compute.util.config import build_config, is_node_host_unique
from xcalar.container.xpu_host import get_config_path


def get_config():
    config_path = get_config_path()
    return build_config(config_path)


def check_node_host_unique(node_id):
    config_path = get_config_path()
    return is_node_host_unique(config_path, node_id)
