#!/usr/bin/env python3
# writer_s3.py

"""Write data to AWS S3"""

from connector_s3 import S3Connector
import datetime
import os


class S3Writer(object):

    def __init__(self, bucket_name):
        self.connector = S3Connector(bucket_name)
        self.key_prefix = "storage/"

    def _get_s3_key(self, cam_id):
        str_today = datetime.date.today().strftime("%y%m%d")
        return self.key_prefix + '/' + str_today + '/' + cam_id + '/'

    def upload_as_public(self, cam_id, filename):
        name = filename.split('/')[-1]
        key = self._get_s3_key(cam_id) + name
        self.connector.upload_local_file_public(filename, key)

    def upload_public_delete_local(self, cam_id, filename):
        self.upload_as_public(cam_id, filename)
        os.remove(filename)

    def upload_not_public(self, cam_id, filename):
        name = filename.split('/')[-1]
        key = self._get_s3_key(cam_id) + name
        self.connector.upload_local_file(filename, key)

    def upload_not_public_delete_local(self, cam_id, filename):
        self.upload_not_public(cam_id, filename)
        os.remove(filename)
