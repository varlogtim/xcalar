import json
from xcalar.external.client import Client
import xcalar.container.context as ctx

client = Client(bypass_proxy=True)
kvstore = client.global_kvstore()

def insert(x, y, key, xpu):
    xpu_r = xpu % (ctx.get_xpu_cluster_size() - 1)
    if ctx.get_xpu_id() == 1 + xpu_r:
        records = json.loads(kvstore.lookup(key))
        for r in records:
            yield r

def get_xpus(x, y):
    xpus = ctx.get_xpu_cluster_size()
    if ctx.get_xpu_id() == xpus - 1:
        yield {'xpus':xpus}
