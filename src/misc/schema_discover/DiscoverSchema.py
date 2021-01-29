import boto3
import uuid
import os
import json
import shutil

# create dslist.txt before running this program
# aws s3 ls --recursive s3://xcfield/instantdatamart/XcalarCloud/ | grep datasets | awk '{print $4}'
class DiscoverSchema():
    def __init__(self, client=None, resource=None, s3client=None):
        #self.role_arn = self.stack_role_arn('DiscoverSchemaStack', 'KinesisServiceRole')
        self.role_arn = os.getenv(
            'KINESISROLEARN',
            'arn:aws:iam::559166403383:role/DiscoverSchemaStack-KinesisServiceRole-13H849V04DH21'
        )
        self.client = client if client else boto3.client('kinesisanalyticsv2')
        self.s3client = s3client if s3client else boto3.client('s3')
        self.resource = resource if resource  else boto3.resource('s3')
        self.SAMPLE_SIZE = 100

    # Get the RoleARN form a CloudFormation stack
    def stack_role_arn(self, stack_name, role_name):
        cfn = boto3.client('cloudformation')
        iam = boto3.client('iam')
        role_res = cfn.describe_stack_resource(
            StackName=stack_name,
            LogicalResourceId=role_name)['StackResourceDetail']
        self.role_arn = iam.get_role(RoleName=role_res['PhysicalResourceId'])['Role']
        return role_arn['Arn']

    def _process_uri(self, s3uri):
        if s3uri.index('s3://') != 0:
            raise Exception("Invalid S3 URI")
        bucket_and_key = s3uri[5:].split('/')
        bucket, key = (bucket_and_key[0], bucket_and_key[1:])
        return bucket, key

    def discover(self, s3uri):
        bucket, key = self._process_uri(s3uri)
        return self.client.discover_input_schema(
            ServiceExecutionRole=self.role_arn,
            S3Configuration={
                'BucketARN': f'arn:aws:s3:::{bucket}',
                'FileKey': '/'.join(key)
            })

    def select_csv(self, **kwargs):
        #print("field delimiter = {}".format(kwargs['FieldDelimiter']))
        resp = self.s3client.select_object_content(
            Bucket=kwargs['bucket'],
            Key=kwargs['fileKey'],
            ExpressionType='SQL',
            #Expression="select * from s3object limit {}".format(self.SAMPLE_SIZE),
            Expression="select * from s3object",
            InputSerialization= {'CompressionType': kwargs['CompressionType'],
                                 'CSV': {
                                     'FileHeaderInfo': kwargs['FileHeaderInfo'],
                                     #'RecordDelimiter': kwargs['RecordDelimiter'],
                                     'FieldDelimiter': kwargs['FieldDelimiter']
                                  }
                                },
            OutputSerialization={'JSON': {
                                     'RecordDelimiter': '\n'
                                     #'FieldDelimiter': kwargs['FieldDelimiter']
                                }
            }
        )
        return resp

    def select_json(self, **kwargs):
        resp = self.s3client.select_object_content(
            Bucket=kwargs['bucket'],
            Key=kwargs['fileKey'],
            ExpressionType='SQL',
            #Expression="select * from s3object limit {}".format(self.SAMPLE_SIZE),
            Expression="select * from s3object",
            #InputSerialization= {'CompressionType': kwargs['CompressionType'],
            #                     'JSON': {
            #                         'Type': jsonType,
            #                      }
            #                    },
            InputSerialization = kwargs['InputSerialization'],
            OutputSerialization={'JSON': {
                                     'RecordDelimiter': '\n'
                                }
            }
        )
        return resp

    def select_parquet(self, **kwargs):
        resp = self.s3client.select_object_content(
            Bucket=kwargs['bucket'],
            Key=kwargs['fileKey'],
            ExpressionType='SQL',
            #Expression="select * from s3object limit {}".format(self.SAMPLE_SIZE),
            Expression="select * from s3object",
            InputSerialization={'Parquet': {}},
            OutputSerialization={'JSON': {}}
        )
        return resp

    def instream(self, ret):
        event_stream = ret['Payload']
        for event in event_stream:
            if 'Records' in event:
                yield event['Records']['Payload']

    def decode_row(self, row_bytes):
        try:
            row = row_bytes.decode('utf-8')
            row_dict = json.loads(row)
            return row_dict
        # XXX catch specific exceptions
        except (UnicodeDecodeError, json.JSONDecodeError) as decode_error:
            # XXX add logging
            raise ValueError("Unable to parse record")

    def row_bytes_generator(self, resp):
        last_hunk = b''
        for dd in self.instream(resp):
            data_rows_bytes = dd.split(b'\n')
            num_rows = len(data_rows_bytes)
            data_rows_processed = 0

            for row_bytes in data_rows_bytes:
                # prepend first row with bytes of last row of previous data
                if data_rows_processed == 0:
                    row_bytes = last_hunk + row_bytes
                data_rows_processed += 1
                # save bytes of last row for beginning of the next data
                if data_rows_processed == num_rows:
                    last_hunk = row_bytes
                    continue
                yield row_bytes
        assert last_hunk == b''

    def longest_value(self, row_dict):
        return max([len(str(v)) for v in row_dict.values()])

    # InputSerialization can be JSON, JSONL, CSV, Parquet
    def discover_fast(self, **kwargs):
        s3uri = kwargs['s3uri']
        bucket, key = self._process_uri(s3uri)
        fileKey = '/'.join(key)
        InputSerialization = kwargs['InputSerialization']
        fileType = kwargs['fileType']
        kwargs['bucket'] = bucket
        kwargs['fileKey'] = fileKey
        if fileType == 'CSV':
            resp = self.select_csv(**kwargs)
        elif fileType == 'Parquet':
            resp = self.select_parquet(**kwargs)
        elif fileType == 'JSON':
            resp = self.select_json(**kwargs)

        data_arr = []
        maxlen = 0
        for row_bytes in self.row_bytes_generator(resp):
            row_dict = self.decode_row(row_bytes)
            lvalue = self.longest_value(row_dict)
            maxlen = lvalue if lvalue > maxlen else maxlen
            data_arr.append(json.dumps(row_dict))

        data = '\n'.join(data_arr)
        data_bytes = bytes(data, 'utf-8')
        tmp_key = "tmp/{}.{}".format(str(uuid.uuid4()), fileType.casefold())
        self.s3client.put_object(Body=data_bytes, Bucket=bucket, Key=tmp_key)
        s3keyurl = "s3://{}/{}".format(bucket, tmp_key)
        ret = self.discover(s3keyurl)
        if InputSerialization == 'CSV':
            ret["InputSchema"]["RecordFormat"]["RecordFormatType"] = "CSV"
        if InputSerialization == 'Parquet':
            ret["InputSchema"]["RecordFormat"]["RecordFormatType"] = "Parquet"
        self.resource.Object(bucket, tmp_key).delete()
        return data, maxlen, ret

class TestSchemas:
    def __init__(self, bucket):
        self.bucket = bucket
        self.discoverSchema = DiscoverSchema()
        self.prefix = "/netstore/datasets/XcalarCloud"
        for fmt in ['JSON', 'Parquet', 'CSV']:
            for meta in ['schema', 'resp', 'data']:
                os.makedirs(f"{self.prefix}/{fmt}/{meta}tmp", exist_ok=True)

    def input_json(self, Type):
        return {'CompressionType': 'None', 'JSON': { 'Type': Type } }

    def input_parquet(self):
        return {'CompressionType': 'None', 'Parquet': {} }

    def test_file(self, data):
        key = data["key"]
        s3uri = "{}://{}/{}".format("s3", self.bucket, key)
        fmt = data["Format"]
        if fmt == "JSON":
            InputSerialization = self.input_json(data["Type"])
        elif fmt == "Parquet":
            InputSerialization = self.input_parquet()
        print(key)
        data, maxlen, resp = self.discoverSchema.discover_fast(s3uri=s3uri, InputSerialization=InputSerialization, fileType='JSON')
        schema = resp['InputSchema']['RecordColumns']
        bname = '_'.join(key.split('/'))
        with open(os.path.join(f"{self.prefix}/{fmt}/datatmp", bname + ".json"), "w") as datafile:
            datafile.write(data + "\n")
        with open(os.path.join(f"{self.prefix}/{fmt}/datatmp", bname + ".longestvalue"), "w") as datafile:
            datafile.write(str(maxlen) + "\n")
        with open(os.path.join(f"{self.prefix}/{fmt}/resptmp", bname), "w") as respfile:
            respfile.write(json.dumps(resp, indent=2) + "\n")
        with open(os.path.join(f"{self.prefix}/{fmt}/schematmp", bname), "w") as schemafile:
            schemafile.write(json.dumps(schema, indent=2) + "\n")


bucket = "xcfield"
sampleData = []
with open("dslist.txt", "r") as dslist:
    datasets = dslist.read().strip().split()
    for ff in datasets:
        fmt = "JSON" if "/json/" in ff else "Parquet"
        tt = "DOCUMENT" if "/json/" in ff else None
        sampleData.append({"key" : ff, "Format" : fmt, "Type" : tt})

testSchemas = TestSchemas(bucket)
for dd in sampleData:
    testSchemas.test_file(dd)
