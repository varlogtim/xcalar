import re
import json
from pathlib import Path
from xcalar.solutions.tools.connector import XShell_Connector, xshell_conf

# get nodes' IP
def read_nodes_from_properties():
    propertyFilePath = XShell_Connector.get_properties()
    if propertyFilePath is not None:
        properties = json.load(open(propertyFilePath))
        if properties.get('xcalar') and properties.get('xcalar').get('nodes'):
            return properties.get('xcalar').get('nodes')
        else:
            return None
    else:
        return None


def get_nodes_from_cfg(file='/etc/xcalar/default.cfg'):

    # ---------------------------------------------------
    # first place: read nodes info from properties file
    # ---------------------------------------------------
    result = None
    try:
        result = read_nodes_from_properties()
    except ValueError:
        # if there is no file, just look for the cfg file
        pass
    if result is not None:
        return result

    # ---------------------------------------------------
    # second place: read nodes info from the xcalar cfg file
    # ---------------------------------------------------
    result = {}
    if not Path(file).is_file():
        return result

    with open(file) as fh:
        data = fh.readlines()

    num_nodes = 0
    match_lines = []
    IpAddr_pattern = r'Node.*.IpAddr='

    for line in data:
        if not line.strip().startswith('//'):
            match1 = re.search(IpAddr_pattern, line)
            if match1:
                match_lines.append(line)

            if 'Node.NumNodes' in line:
                (_, v1) = line.strip().split('=')
                num_nodes = v1

    for line in match_lines:
        for n in range(int(num_nodes)):
            pattern = f'Node.{n}.IpAddr'
            match2 = re.search(pattern, line)
            if match2:
                (k2, v2) = line.strip().split('=')
                k2 = k2.split('.')[1]
                result[k2] = v2
    return result



def get_nodes_dict():
    hosts_dict = get_nodes_from_cfg()
    return hosts_dict


def get_nodes_list():
    hosts_dict = get_nodes_from_cfg()
    hosts_list = [host for key, host in hosts_dict.items()]
    return hosts_list


if __name__ == '__main__':
    remote_nodes = get_nodes_from_cfg('/tmp/default.cfg')
    hostname = remote_nodes.get(0)
    print(hostname)
