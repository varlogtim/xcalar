import pytz
est = pytz.timezone('US/Eastern')
def convert2EST(colName):
    return colName.astimezone(est).strftime("%m/%d/%Y, %H:%M:%S")


def parse_sz(col):
    d = eval(col.split('Table info: ')[-1])
    maps = [f'{m}~{v["size"]}~{v["rows"]}' for m,v in d.items()]
    return '^'.join(maps)


def parse_wmk(col):
    d = eval(col.split('Watermark: ')[-1])
    maps = [f'{m}~{v}' for m,v in d.items() if v != None]
    return '^'.join(maps) 

def parse_opts(col):
    d = eval(col.split('Params: ')[-1])['opts']
    maps = '^'.join([f'{m}~{v}' for m,v in d["metadataInfo"].items()])
    out = f'{d["batch_id"]}|{d["maxIter"]}|{maps}'
    return out

def parse_props(col):
    d = eval(col)
    return f'{d["kafka_props"]}|{d["partitionInfo"]}|{d["topic"]}'
    
