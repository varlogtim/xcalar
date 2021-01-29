CLUSTER_INFO_KEY = "/sys/clusterGeneration"


def get_cluster_info(client):
    kvstore = client.global_kvstore()
    try:
        return kvstore.lookup(CLUSTER_INFO_KEY)
    except Exception:
        return None
