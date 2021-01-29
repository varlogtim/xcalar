import os
from socket import gethostname

from xcalar.compute.util.Qa import (XcalarQaS3ClientArgs, DefaultTargetName,
                                    buildS3Path)

from xcalar.external.client import Client
from xcalar.external.dataflow import Dataflow

workbookName = 'data_target_workbook'
s3_targetName = 'QA S3 minio'
s3_access_key = XcalarQaS3ClientArgs['aws_access_key_id']
s3_secret_access_key = XcalarQaS3ClientArgs['aws_secret_access_key']
s3_endpoint_url = XcalarQaS3ClientArgs['endpoint_url']
s3_region_name = XcalarQaS3ClientArgs['region_name']
s3_verify = 'true' if XcalarQaS3ClientArgs['verify'] else 'false'

s3_exportPath = buildS3Path(
    os.path.join("targetExport", gethostname(), "data_target_test.csv"))
datasetName = 'transaction'
# datasetPath = buildNativePath(file_path)
datasetPath = 'netstore/datasets/qa/small.csv'

# PYTEST


# Part of this code will be visible in the document
def test_data_target_demo():
    # [START data_target]

    # from xcalar.external.client import Client
    # from xcalar.external.dataflow import Dataflow
    client = Client()
    # Add a s3 data target with specified credentials
    client.add_data_target(
        target_name=s3_targetName,
        target_type_id='s3fullaccount',
        params={
            'access_key': s3_access_key,    # '<your access key here>'
            'secret_access_key': s3_secret_access_key,    # '<your s_a_k here>'
            'endpoint_url': s3_endpoint_url,    # '<your endpoint url here>'
            'region_name': s3_region_name,    # '<region name here>'
            'verify': s3_verify,    # 'True or False'
        })

    workbook = client.create_workbook(workbook_name=workbookName)

    # Load a table from elsewhere and export it to s3 data target

    db = workbook.build_dataset(
        dataset_name=datasetName,
        data_target=DefaultTargetName,
        path=datasetPath,
        import_format='csv')
    db.option("schemaMode", 'header')
    db.option('fieldDelim', ',')
    dataset = db.load()

    dataflow = Dataflow.create_dataflow_from_dataset(
        client=client, dataset=dataset)
    session = workbook.activate()

    table = 'transactionTable'
    session.execute_dataflow(dataflow, table, is_async=False)

    # Chose what columns you want to export
    columns = [("p::Amount Paid", "Amount Paid"),
               ("p::Phone Number", "Phone Number"),
               ("p::Transaction Date", "Transaction Date")]

    # Export table to s3 data target with built-in driver
    table = session.get_table(table_name=table)
    table.export(
        columns=columns,
        driver_name='single_csv',
        driver_params={
            'file_path': s3_exportPath,
            'target': s3_targetName
        })
    # [END data_target]
    table.drop()

    # Load the file back to make sure it works
    db = workbook.build_dataset("csv_export_test", s3_targetName,
                                s3_exportPath, 'csv')
    db.option("fieldDelim", ',')
    dataset = db.load()

    assert len(list(dataset.records())) == 99

    client.get_data_target(s3_targetName).delete()

    for workbook in client.list_workbooks():
        workbook.delete()

    for dataset in client.list_datasets():
        dataset.delete()
