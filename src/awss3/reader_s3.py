#!/usr/bin/env python3
# reader_s3.py

"""Read data from AWS S3"""

from connector_s3 import S3Connector


class S3Reader(object):

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.connector = S3Connector(self.bucket_name)


class S3VideoReader(S3Reader):

    def __init__(self, bucket_name):
        super.__init__(bucket_name)
        self.http_prefix = 'http://s3-us-west-2.amazonaws.com/' + self.bucket_name + '/'

    def get_urls_in_folder(self, prefix):
        keys = self.connector.get_objs_keys_with_prefix(prefix)
        return [self.http_prefix + key for key in keys]

    def get_url_with_key(self, key):
        return self.http_prefix + key
