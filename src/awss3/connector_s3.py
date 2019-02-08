#!/usr/bin/env python3
# connector_s3.py

"""Binding AWS S3"""

import boto3
import botocore


class S3Connector(object):

    def __init__(self, bucket_name):
        try:
            self.bucket_name = bucket_name
            # low-level functional API
            self.client = boto3.client('s3')
            # high-level object-oriented API
            self.resource = boto3.resource('s3')
            self.bucket = self.resource.Bucket(self.bucket_name)
        except botocore.exceptions.PartialCredentialsError:
            print("Please set up AWS authentication credentials")

    def get_objs_with_prefix(self, prefix):
        return list(self.bucket.objects.filter(Prefix=prefix))

    def get_objs_keys_with_prefix(self, prefix):
        objs = self.get_objs_with_prefix(prefix)
        keys = [obj.key for obj in objs if len(obj.key) > len(prefix)]
        return keys

    def get_object_with_key(self, key):
        return self.client.get_object(Bucket=self.bucket_name, Key=key)

    def get_object_body(self, key):
        obj = self.client.get_object(Bucket=self.bucket_name, Key=key)
        return obj['Body']

    def get_object_size(self, key):
        obj = self.client.get_object(Bucket=self.bucket_name, Key=key)
        return obj['ContentLength']

    def upload_local_file(self, filename, key):
        self.bucket.upload_file(filename, key)

    def upload_local_file_public(self, filename, key):
        self.bucket.upload_file(filename, key, ExtraArgs={'ACL': 'public-read'})

    def upload_file_obj(self, fileobj, key):
        self.bucket.upload_fileobj(fileobj, key)

    def download_to_local_file(self, key, filename):
        try:
            self.bucket.download_file(key, filename)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

    def download_file_obj(self, key, fileobj):
        '''data should be a file-like-object'''
        try:
            self.bucket.download_fileobj(key, fileobj)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

    def move_obj_within(self, from_key, to_key):
        try:
            copy_source = {
                'Bucket': self.bucket_name,
                'Key': from_key
                }
            self.resource.Object(self.bucket_name, to_key).copy(copy_source)
            self.resource.Object(self.bucket_name, from_key).delete()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise
