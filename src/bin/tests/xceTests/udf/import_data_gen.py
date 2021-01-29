import json
import time
import datetime
import random
from faker import Faker


def gen_data(filepath, instream, config):
    inObj = json.loads(instream.read())
    if inObj['numRows'] == 0:
        return
    start = inObj['startRow'] + 1
    end = start + inObj['numRows']

    seed = config.get('seed', int(time.time())) + start
    random.seed(seed)
    fake = Faker()
    fake.seed(seed)
    schema = config['schema']
    columns = schema["columns"]
    while start < end:
        res = {}
        idx = 0
        for col_name, col_type in columns.items():
            value = random.randint(start, end)
            if col_type.lower() == "integer":
                value = value
            elif col_type.lower() == 'float':
                value = value * random.random()
                value = round(value, 3)
            elif col_type.lower() == 'money':
                value = value * random.random()
                value = str(round(value, 2))
            elif col_type.lower() == 'timestamp':
                value = datetime.datetime.now().timestamp() * 1000
            elif col_type.lower() == 'string':
                value = fake.text()
            null_weight = random.randint(0, 10)
            res[col_name] = random.choices(
                [value, None], weights=[10 - null_weight, null_weight])[0]
            res[col_name] = value
            idx += 1
        yield res
        start += 1
