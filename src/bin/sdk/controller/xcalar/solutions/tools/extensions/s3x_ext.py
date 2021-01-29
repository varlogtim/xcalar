import boto3
from botocore.exceptions import ClientError
import sys
import subprocess
import os
from smart_open import open
from colorama import Style
import traceback

from xcalar.solutions.tools.arg_parser import ArgParser
from xcalar.solutions.tools.connector import XShell_Connector
from xcalar.solutions.query_builder import QueryBuilder

argp = ArgParser('s3x', [
    {
        'verbs': {
            's3_path': 'full s3 url to file',
            'shard_size': 'desired size of each shard in MB',
            'output_dir': 'desired s3 path of output directory'
        },
        'desc': 'shard S3 file'
    }
])

def dispatcher(line):
    usage = argp.get_usage()
    input_params = argp.get_params(line)

    try:
        if 's3_path' in input_params:
            params = argp.init_params(
                input_params, defaults={
                    's3_path': '',
                    'shard_size': 10,
                    'output_dir': ''
                })
            sharder(params)
        else:
            print(usage)
            return
    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as err:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {err}')
        traceback.print_tb(err.__traceback__)

def sharder(params):
    url = params['s3_path']
    shard_size = int(params['shard_size'])
    output_dir_path = params['output_dir']
    #####
    bucket_name = url.split("/")[2]
    input_file_name = '/'.join(url.split('/')[3:])
    output_bucket = output_dir_path.split("/")[2]
    output_dir = '/'.join(output_dir_path.split("/")[3:])
    if output_dir[-1] == '/':
        output_dir = output_dir[:-1]
    #output_dir = input_file_name.split('.')[0] + "_sharded"
    #####
    s3 = boto3.resource('s3')
    shard_size = shard_size * 1000000 # conversion to bytes
    bytes_written = 0
    total_bytes_written = 0
    file_count = 0
    obj = s3.Object(bucket_name, input_file_name)
    total_file_size = obj.content_length
    data = ''
    header = ''
    header_iteration = True
    for line in open(url):
        data += line
        if header_iteration:
            header = line
            header_iteration = False
        bytes_written += len(line.encode('utf-8'))
        if ((bytes_written > shard_size)):
            s3.meta.client.put_object(Body=data, Bucket=bucket_name, Key=f'{output_dir}/shard{file_count}.csv')
            print(f'uploaded {output_dir}/shard{file_count}.csv')
            file_count += 1
            total_bytes_written += bytes_written
            bytes_written = len(header.encode('utf-8'))
            data = header
    if bytes_written > 0:
        s3.meta.client.put_object(Body=data, Bucket=bucket_name, Key=f'{output_dir}/shard{file_count}.csv')
        print(f'uploaded {output_dir}/shard{file_count}.csv')

def load_ipython_extension(ipython, *args):
    argp.register_magic_function(ipython, dispatcher)

def unload_ipython_extension(ipython):
    pass
if __name__ == '__main__':
    params = {'s3_path':sys.argv[1], 'shard_size': sys.argv[2]}
    sharder(params)