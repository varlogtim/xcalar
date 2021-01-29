from xcalar.external.client import Client

workbookName = 'streaming_udf_workbook'
datasetName = 'dataset_demo'
udfName = 'streaming_udf_demo'

# The udf function
memUdf = '''
import random
from datetime import datetime, timedelta
import time
import json

def randdate():
    d1 = datetime.strptime('1/1/1980', '%m/%d/%Y')
    d2 = datetime.strptime('1/10/2019', '%m/%d/%Y')

    d3 = d1 + timedelta(
        seconds=random.randint(0, int((d2 - d1).total_seconds()))
    )
    return d3.strftime('%Y-%m-%d')

def parseData(fullPath, inStream):
    inObj = json.loads(inStream.read())
    numLocalRows = inObj['numRows']
    startNum = inObj['startRow']
    rowGen = random.Random()
    rowGen.seed(537)
    for ii in range(startNum, startNum+numLocalRows):
        rowDict = {'name': ii, 'birthday': randdate()}
        yield rowDict
'''

memTargetName = 'QA Shared Target'

# PYTEST


# Part of this code will be visible in the document
def test_streaming_udf_demo():
    # [START streaming_udf]
    # from xcalar.external.udf import UDF

    # from xcalar.external.client import Client
    # from xcalar.external.result_set import ResultSet

    client = Client()

    # Use memory type data target to automatically generate dataset
    # with no backing store
    client.add_data_target(
        target_name=memTargetName, target_type_id='memory', params={})
    workbook = client.create_workbook(workbook_name=workbookName)
    df = workbook.create_udf_module(udfName, memUdf)

    # Use customized stream udf to parse the data
    db = workbook.build_dataset(
        dataset_name=datasetName,
        data_target=memTargetName,
        path='1000',
        import_format='udf',
        parser_name='{}:parseData'.format(udfName))

    # Load the dataset and view the result
    dataset = db.load()
    dataset.show()
    # [END streaming_udf]

    for workbook in client.list_workbooks():
        workbook.delete()
    for dataset in client.list_datasets():
        dataset.delete()
