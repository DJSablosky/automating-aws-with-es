#!/usr/bin/python
# -*- coding:  utf-8 -*-

"""Elkotron:  Streams log files to Elasticsearch.

Elkotron streams log files from an S3 bucket to Elasticsearch.
-ELB logs
-VPC logs
"""

import boto3
import re
import requests
from requests_aws4auth import AWS4Auth
from pprint import pprint
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
import shlex
import gzip
import io
import os
import fnmatch


region = 'us-east-1'  # e.g. us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key,
                   region, service, session_token=credentials.token)

hosts = ["elastic-1.ardentmc.com", "elastic-2.ardentmc.com",
         "elastic-3.ardentmc.com"]
index_names = ["elb-log-index", "vpc-log-index"]
elb_type_name = "elb-log-type"
vpc_type_name = "vpc-log-type"

headers = {"Content-Type": "application/json"}

s3 = boto3.client('s3')

es = Elasticsearch(
    hosts=hosts,
    http_auth=awsauth,
    use_ssl=False,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

# Regular expressions used to parse some simple log lines
ip_pattern = re.compile('(\d+\.\d+\.\d+\.\d+)')
# time_pattern = re.compile('\[(\d+\/\w\w\w\/\d\d\d\d:\d\d:\d\d:\d\d\s-\d\d\d\d)\]')
# iso8601 time_pattern
time_pattern = re.compile(r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$')
message_pattern = re.compile('\"(.+)\"')


def decode_elb_log(lines):
    for line in lines:
        log = shlex.split(line)
        client_port = ip_pattern.search(line).group(1)
        # timestamp = time_pattern.search(line).group(1)
        log = shlex.split(line)
        timestamp = log[1]
        message = message_pattern.search(line).group(1)

        # Index for elasticsearch
        elb_fields_key = ('client_port', 'timestamp', 'message')
        elb_fields_vals = (client_port, timestamp, message)

        # Return a dictionary holding values from each lines
        elb_logs_d = dict(zip(elb_fields_key, elb_fields_vals))

        # Return the row on each iteration
        yield index_names[0], elb_logs_d


def decode_vpc_log(lines):
    for line in lines:
        log = shlex.split(line)

        # Index for elasticsearch
        vpc_fields_key = ('version', 'account_id', 'interface_id', 'srcaddr',
                          'dstaddr', 'srcport', 'dstport', 'protocol',
                          'packets', 'transfer_bytes', 'start', 'end', 'action',
                          'log_status')

        vpc_fields_vals = (log[0], log[1], log[2], log[3], log[4], log[5],
                           log[6], log[7], log[8], log[9], log[10], log[11],
                           log[12], log[13])

        # Return a dictionary holding values from each lines
        vpc_logs_d = dict(zip(vpc_fields_key, vpc_fields_vals))

        # Return the row on each iteration
        yield index_names[1], vpc_logs_d


# Lambda execution starts here
def handler(event, context):
    for index_name in index_names:
        if not es.indices.exists(index_name):
            # since we are running locally, use one shard and no replicas
            request_body = {
                "settings":  {
                    "number_of_shards":  7,
                    "number_of_replicas":  2
                }
            }
            pprint("creating '%s' index..." % (index_name))
            index_res = es.indices.create(index=index_name, body=request_body)
            pprint(" response:  '%s'" & (index_res))

    for record in event['Records']:

        # Get the bucket name and key for the new file
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        if fnmatch.fnmatch(key, '*.log.gz'):

            # Get, read, and split the file into lines
            obj = s3.get_object(Bucket=bucket, Key=key)

            # Read compressed data from log.gz file
            buffer = io.BytesIO(obj['Body'].read())
            try:
                body = gzip.GzipFile(None, 'rb', fileobj=buffer).read().decode('utf-8')
            except OSError:
                buffer.seek(0)
                body = buffer.read()
            lines = body.splitlines()

            bulk_body = ''

            if 'elasticloadbalancing' in key:
                gendata = ({
                    "_index":  index_names[0],
                    "_type":  elb_type_name,
                    "_source":  elb_logs_d,
                } for index_names[0], elb_logs_d in decode_elb_log(lines))
                helpers.bulk(es, gendata)

            elif 'vpcflowlogs' in key:
                gendata = ({
                    "_index":  index_names[1],
                    "_type":  vpc_type_name,
                    "_source":  vpc_logs_d,
                } for index_names[1], vpc_logs_d in decode_vpc_log(lines))
                helpers.bulk(es, gendata)
