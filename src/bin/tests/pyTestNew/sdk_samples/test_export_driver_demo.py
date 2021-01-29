import tempfile
import shutil

from xcalar.compute.util.Qa import DefaultTargetName

from xcalar.external.client import Client
from xcalar.external.dataflow import Dataflow

from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiColumnT

workbookName = 'export_demo'
datasetName = 'dataset_demo'

# Test cases which try loading these datasets
typedColumns = [('reviewID', 'DfString'), ('record_type', 'DfString'),
                ('business_name', 'DfString'), ('userID', 'DfString'),
                ('stars', 'DfFloat64'), ('price_range', 'DfInt64'),
                ('date', 'DfString')]
rawCSV = """\
R3664999,review,Larry's Diner,U638276,3.9,4,6/25/2012
R2412340,review,Grasshopper,U677889,4.2,3,5/1/2009
R6180480,review,Anong Inc.,U131481,2.8,1,9/14/2000
R1678237,review,Las Brisas Del Mar,U769140,1.7,2,5/18/2015
R7963826,review,Pulcinella Pizzeria,U190100,3.7,4,11/21/2003
R8661642,review,The Garrison,U508568,5.0,1,1/27/2002
R3136675,review,Lena's Pizza & Sub Shop,U463446,4.1,2,10/1/2011
R3245727,review,St. Lawrence Gallery Cafe,U149027,2.6,1,3/28/2008
R8292335,review,Realdo's Pizzeria & Restaurant,U132594,1.8,3,10/6/2012
"""
schema = [
    XcalarApiColumnT(colName, colName, colType)
    for (colName, colType) in typedColumns
]

# PYTEST


# Part of this code will be visible in the document
def test_export_driver():
    tmpf = tempfile.NamedTemporaryFile('w', encoding='utf-8')
    tmpf.write(rawCSV)
    tmpf.flush()
    datasetPath = tmpf.name

    outputDir = tempfile.mkdtemp()
    # [START export_driver]
    # from xcalar.external.client import Client
    # from xcalar.external.dataflow import Dataflow

    client = Client()

    # Create a new workbook
    workbook = client.create_workbook(workbook_name=workbookName)

    # Load dataset with specific parameters
    db = workbook.build_dataset(
        dataset_name=datasetName,
        data_target=DefaultTargetName,
        path=datasetPath,
        import_format='csv')

    db.option('fieldDelim', ',')
    db.option('schemaMode', 'loadInput')
    db.option('schema', schema)
    dataset = db.load()

    # Create dataflow
    dataflow = Dataflow.create_dataflow_from_dataset(
        client=client, dataset=dataset)

    # Execute the dataflow
    session = workbook.activate()
    table = 'niceReview'
    session.execute_dataflow(dataflow, table, is_async=False)

    columns = [("business_name", "Business Name"), ("date", "Date"),
               ("stars", "Stars")]

    # Use built-in export driver to divide/export the table into multiple csv file
    table = session.get_table(table_name=table)
    table.export(
        columns=columns,
        driver_name='multiple_csv',
        driver_params={
            'directory_path': outputDir,
            'target': DefaultTargetName,
            'file_base': 'transaction'
        })

    # [END export_driver]
    tmpf.close()
    shutil.rmtree(outputDir)

    table.drop()
    workbook.delete()
    for dataset in client.list_datasets():
        dataset.delete()
