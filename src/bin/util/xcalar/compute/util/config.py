"""Manage Xcalar cluster config files

Provides for interacting with Xcalar cluster config files, including finding
their locations and parsing them. Typically this is not done directly, but is
implicitly done when managing a running cluster, through .cluster, a sister
module to this one
"""
import os
import socket

# Config file keys
NUM_NODES_KEY = "Node.NumNodes"
IP_KEY = "IpAddr"
PORT_KEY = "Port"
API_PORT_KEY = "ApiPort"
MONITOR_PORT_KEY = "MonitorPort"
ROOT_PATH_KEY = "Constants.XcalarRootCompletePath"
LOG_PATH_KEY = "Constants.XcalarLogCompletePath"

NODE_INFO_KEY_FMT = "Node.{node_id}.{field}"

# Config paths
PROD_DEFAULT_CONFIG_PATH = "/etc/xcalar/default.cfg"
DEV_CONFIG_PATH_FMT = "src/data/{hostname}.cfg"
DEV_DEFAULT_CONFIG_PATH = "src/data/test.cfg"


def detect_config():
    """Detect and build the appropriate config object"""
    config_path = _find_config_file()
    return build_config(config_path)


def build_config(config_path):
    """Build a config object from a known path"""
    with open(config_path, "r") as f:
        config_dict = _parse_config(f)

    config = ClusterConfig(config_path, config_dict)

    return config


def is_node_host_unique(config_path, my_node_id):
    """Is node host unique"""
    with open(config_path, "r") as f:
        config_dict = _parse_config(f)
    config = ClusterConfig(config_path, config_dict)
    node_dicts = config.nodes
    my_node_host = node_dicts[my_node_id][IP_KEY]

    for node_id in range(config.num_nodes):
        if node_id != my_node_id and node_dicts[node_id][
                IP_KEY] == my_node_host:
            return False
    return True


def _parse_config(config_file):
    config_params = {}
    for line in config_file:
        line = line.strip()
        if "=" in line and not line.startswith("//") and not line.startswith(
                "#"):
            name, var = line.partition("=")[::2]
            config_params[name] = var.strip()
    return config_params


# Generate all potential config paths. We do this as a generator function
# because in some niche situations we've seen socket.gethostname() hang
def _potential_config_paths():
    if "XCE_CONFIG" in os.environ:
        yield os.environ.get("XCE_CONFIG")

    yield PROD_DEFAULT_CONFIG_PATH

    if "XLRDIR" in os.environ:
        xlr_dir = os.environ["XLRDIR"]

        # Host-specific config
        hostname = socket.gethostname()
        host_rel_path = DEV_CONFIG_PATH_FMT.format(hostname=hostname)
        yield os.path.join(xlr_dir, host_rel_path)

        # Default config
        yield os.path.join(xlr_dir, DEV_DEFAULT_CONFIG_PATH)


def _find_config_file():
    for path in _potential_config_paths():
        if path and os.path.isfile(path):
            return path
    raise RuntimeError("No config file found")


def get_cluster_node_list():
    config = detect_config()
    configDict = {}

    parsedConfig = config.all_options
    for name in parsedConfig:
        value = parsedConfig[name]
        if (name.endswith('IpAddr')):
            hostSplit = name.split('.')
            configDict[hostSplit[1]] = value
    return configDict


class ClusterConfig:
    def __init__(self, config_file_path, config_dict):
        self._config_file_path = config_file_path
        self._config_dict = config_dict

    @property
    def config_file_path(self):
        return self._config_file_path

    @property
    def num_nodes(self):
        return int(self._config_dict[NUM_NODES_KEY])

    @property
    def nodes(self):
        node_dicts = []
        for node_id in range(self.num_nodes):
            node_dict = {}

            ip_key = NODE_INFO_KEY_FMT.format(node_id=node_id, field=IP_KEY)
            port_key = NODE_INFO_KEY_FMT.format(
                node_id=node_id, field=PORT_KEY)
            api_port = NODE_INFO_KEY_FMT.format(
                node_id=node_id, field=API_PORT_KEY)
            monitor_port = NODE_INFO_KEY_FMT.format(
                node_id=node_id, field=MONITOR_PORT_KEY)

            node_dict[IP_KEY] = self._config_dict[ip_key]
            node_dict[PORT_KEY] = self._config_dict[port_key]
            node_dict[API_PORT_KEY] = self._config_dict[api_port]
            node_dict[MONITOR_PORT_KEY] = self._config_dict[monitor_port]

            node_dicts.append(node_dict)
        return node_dicts

    @property
    def xcalar_root_path(self):
        return self._config_dict[ROOT_PATH_KEY]

    @property
    def xcalar_logs_path(self):
        return self._config_dict[LOG_PATH_KEY]

    @property
    def all_options(self):
        return self._config_dict
