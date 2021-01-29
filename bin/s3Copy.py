#!/usr/bin/env python2.7
from urlparse import urlparse, parse_qs
import time
import os
import sys
import multiprocessing
import itertools
import argparse

import boto3

# Global variables
client = None
bucket = None

def writeFile(objs):
    fullFn = objs[0]
    key = objs[1]
    print "uploading '{}'...".format(key)
    try:
        with open(fullFn, 'r') as f:
            client.upload_fileobj(f, bucket, key)
    except Exception as e:
        print "error with {}: {}".format(key, e)
        pass

def uploadDir(dirPath):
    copyObjs = []
    for root, dirs, filenames in os.walk(dirPath):
        for fn in filenames:
            fullFn = os.path.join(root, fn)
            key = os.path.relpath(fullFn, dirPath)
            copyObjs.append((fullFn, key))

    pool = multiprocessing.Pool(multiprocessing.cpu_count() * 2)
    pool.map(writeFile, copyObjs, 20)

def getKeys(buck):
    listArgs = {}
    listArgs["Bucket"] = buck

    while True:
        listDict = client.list_objects_v2(**listArgs)
        for s3Obj in listDict.get("Contents", []):
            key = s3Obj["Key"]
            yield key

        if listDict["IsTruncated"]:
            listArgs["ContinuationToken"] = listDict["NextContinuationToken"]
            continue
        break

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def createBucket(buck):
    try:
        client.create_bucket(Bucket=buck)
    except Exception as e:
        if "BucketAlreadyOwnedByYou" in str(e):
            pass
        else:
            raise

def cleanBucket(buck):
    g = getKeys(buck)
    keyList = list(g)
    for keys in batch(keyList, 1000):
        delArgs = {
            "Bucket": buck,
            "Delete": {
                "Objects": [{"Key": k} for k in keys]
            }
        }
        print "deleting {} keys".format(len(keys))
        client.delete_objects(**delArgs)

def main():
    global client
    global bucket
    parser = argparse.ArgumentParser(description="Manage S3 QA infrastructure")
    # Defaults here connect to local minio
    parser.add_argument("-c", "--accessKey",
                        default='K3CC2YMWIGSZVR09Q3WO',
                        help="S3 access key")
    parser.add_argument("-s", "--secretKey",
                        default='xqfitN99bb6l2nZRTcOqvsTrR66gCCSKaQssSu4Z',
                        help="S3 secret key")
    parser.add_argument("-u", "--endpointUrl",
                        default='https://minio.int.xcalar.com',
                        help="Endpoint url for S3 service")
    parser.add_argument("-r", "--regionName",
                        default='us-east-1',
                        help="Region name for S3 service")
    parser.add_argument("-v", "--verifySsl",
                        action='store_true',
                        help="Check SSL certificate for service")
    parser.add_argument("-b", "--bucket",
                        default='qa.xcalar',
                        help="Bucket to copy data into")
    parser.add_argument("-x", "--cleanBucket",
                        action="store_true",
                        help="Empty all keys in bucket before copying (PERMANENT)")
    parser.add_argument("-d", "--qaDir",
                        default=os.path.join(os.getcwd(), "buildOut", "src", "data", "qa"),
                        help="Directory to copy to S3")
    args = parser.parse_args()

    clientArgs = {
        'aws_access_key_id': args.accessKey,
        'aws_secret_access_key': args.secretKey,
        'endpoint_url': args.endpointUrl,
        'region_name': args.regionName,
        'verify': args.verifySsl
    }
    client = boto3.client("s3", **clientArgs)
    bucket = args.bucket

    createBucket(bucket)

    if args.cleanBucket:
        cleanBucket(bucket)

    start = time.time()
    uploadDir(args.qaDir)
    duration = time.time() - start
    print "Uploaded in {}s".format(duration)

if __name__ == "__main__":
    sys.exit(main())
