# coding: utf-8
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

session = boto3.Session(profile_name = 'corpnew')

region = 'us-east-1' # e.g. us-west-1
service = 'es'
awsauth = AWS4Auth(AccessKeyId, SecretAccessKey, region, service)
hosts = ["elastic-1.ardentmc.com", "elastic-2.ardentmc.com", "elastic-3.ardentmc.com"]
index_names = ["elb-log-index", "vpc-log-index"]
elb_type_name = "elb-log-type"
vpc_type_name = "vpc-log-type"

headers = { "Content-Type": "application/json" }

s3 = boto3.client('s3')
s3Resource = boto3.resource('s3')

es = Elasticsearch(
    hosts=hosts,
    http_auth=awsauth,
    use_ssl=False,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)
print(es.info())
# Regular expressions used to parse some simple log lines
ip_pattern = re.compile('(\d+\.\d+\.\d+\.\d+)')
#time_pattern = re.compile('\[(\d+\/\w\w\w\/\d\d\d\d:\d\d:\d\d:\d\d\s-\d\d\d\d)\]')
#iso8601 time_pattern
time_pattern = re.compile(r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$')
message_pattern = re.compile('\"(.+)\"')
