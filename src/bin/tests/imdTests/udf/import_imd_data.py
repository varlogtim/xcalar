import json
import time
import random
from faker import Faker

import xcalar.compute.util.imd.imd_constants as ImdConstant


def genData(filepath, instream, config):
    inObj = json.loads(instream.read())
    if inObj['numRows'] == 0:
        return
    start = inObj['startRow'] + 1
    end = start + inObj['numRows']

    seed = config.get('seed', int(time.time())) + start
    random.seed(seed)
    fake = Faker()
    fake.seed(seed)
    while start < end:
        res = {}
        idx = 0
        key = random.randint(start, end)
        rank = int(time.time() * 1000000)
        for s in config['schema']:
            col = s['name']
            value = random.randint(start, end)
            pk = key
            if s['type'].lower() == "integer":
                value = value
                pk = key
            elif s['type'].lower() == 'float':
                value = value * random.random()
                value = round(value, 3)
                pk = key * 1.1
            elif s['type'].lower() == 'money':
                value = value * random.random()
                value = str(round(value, 2))
                pk = key * 1.11
                pk = str(round(pk, 2))
            elif s['type'].lower() == 'string':
                value = fake.text()
                pk = "imd test pk " + str(key)
            # XXX with a small chance it could be null
            # UNCOMMENT THIS ONCE BACKEND SUPPORTS NULL PROPERLY
            # res[col] = random.choices([value, None], weights=[9, 1])[0]
            res["PK" + str(idx)] = pk
            res[col] = value
            idx += 1
        res[ImdConstant.Opcode] = random.choices([1, 0], weights=[7, 3])[0]
        res[ImdConstant.Rankover] = rank
        yield res
        start += 1
